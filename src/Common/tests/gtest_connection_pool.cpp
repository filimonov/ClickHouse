#include <IO/ReadWriteBufferFromHTTP.h>
#include <Common/CurrentThread.h>
#include <Common/HTTPConnectionPool.h>

#include <Poco/URI.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerRequestImpl.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>

#include <gtest/gtest.h>

namespace
{

template<class T>
class SafeHandler
{
public:
    using Ptr = std::shared_ptr<SafeHandler<T>>;

    SafeHandler() = default;
    SafeHandler(SafeHandler<T>&) = delete;
    SafeHandler& operator=(SafeHandler<T>&) = delete;

    T get()
    {
        std::lock_guard lock(mutex);
        return obj;
    }

    void set(T && options_)
    {
        std::lock_guard lock(mutex);
        obj = std::move(options_);
    }

protected:
    std::mutex mutex;
    T obj = {};
};

struct RequestOptions
{
    size_t slowdown_receive = 0;
    int overwrite_keep_alive_timeout = 0;
    int overwrite_keep_alive_max_requests = 10;
    bool response_no_keep_alive = false;
    size_t trailing_garbage_bytes = 0;
};

size_t stream_copy_n(std::istream & in, std::ostream & out, std::size_t count = std::numeric_limits<size_t>::max())
{
    const size_t buffer_size = 4096;
    char buffer[buffer_size];

    size_t total_read = 0;

    while (count > buffer_size)
    {
        in.read(buffer, buffer_size);
        size_t read = in.gcount();
        out.write(buffer, read);
        count -= read;
        total_read += read;

        if (read == 0)
            return total_read;
    }

    in.read(buffer, count);
    size_t read = in.gcount();
    out.write(buffer, read);
    total_read += read;

    return total_read;
}

class MockRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit MockRequestHandler(SafeHandler<RequestOptions>::Ptr options_)
        : options(options_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        int value = request.getKeepAliveTimeout();
        ASSERT_GT(value, 0);

        auto params = options->get();

        if (params.overwrite_keep_alive_timeout > 0)
            response.setKeepAliveTimeout(params.overwrite_keep_alive_timeout, params.overwrite_keep_alive_max_requests);

        if (params.response_no_keep_alive)
            response.setKeepAlive(false);

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        auto size = request.getContentLength();
        if (size > 0)
            response.setContentLength(size); // ContentLength is required for keep alive
        else
            response.setChunkedTransferEncoding(true); // or chunk encoding

        if (params.slowdown_receive > 0)
            sleepForSeconds(params.slowdown_receive);

        if (params.trailing_garbage_bytes > 0)
        {
            /// Hand-assemble the whole response (status line, headers, body, and trailing
            /// garbage) into one buffer and send it as a single raw write, bypassing
            /// response.send()'s own framed stream entirely. Two separate writes (the framed
            /// response, then garbage direct to the socket) can arrive as two separate reads on
            /// the client, leaving nothing buffered; one write removes that race, so the
            /// client's HTTP_DEFAULT_BUFFER_SIZE (8 KiB) read-ahead reliably pulls the garbage
            /// into its buffer alongside the response body.
            std::string body(static_cast<size_t>(size), '\0');
            request.stream().read(body.data(), size);

            std::ostringstream raw_response;
            response.write(raw_response);
            raw_response << body << String(params.trailing_garbage_bytes, 'x');

            auto & request_impl = static_cast<Poco::Net::HTTPServerRequestImpl &>(request);
            String raw = raw_response.str();
            request_impl.socket().sendBytes(raw.data(), static_cast<int>(raw.size()));
            return;
        }

        stream_copy_n(request.stream(), response.send(), size);
    }

    SafeHandler<RequestOptions>::Ptr options;
};

class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    explicit HTTPRequestHandlerFactory(SafeHandler<RequestOptions>::Ptr options_)
        : options(options_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandler(options);
    }

    SafeHandler<RequestOptions>::Ptr options;
};

}

using HTTPSession = Poco::Net::HTTPClientSession;
using HTTPSessionPtr = std::shared_ptr<Poco::Net::HTTPClientSession>;

class ConnectionPoolTest : public testing::Test {
protected:
    ConnectionPoolTest()
    {
        options = std::make_shared<SafeHandler<RequestOptions>>();

        startServer();
    }

    void SetUp() override {
        timeouts = DB::ConnectionTimeouts();
        DB::HTTPConnectionPools::Limits def_limits{};
        DB::HTTPConnectionPools::instance().setLimits(def_limits, def_limits, def_limits);

        options->set(RequestOptions());

        DB::HTTPConnectionPools::instance().dropCache();
        DB::CurrentThread::getProfileEvents().resetCounters();
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }

    DB::IHTTPConnectionPoolForEndpoint::Ptr getPool()
    {
        auto uri = Poco::URI(getServerUrl());
        return DB::HTTPConnectionPools::instance().getPool(DB::HTTPConnectionGroupType::HTTP, uri, DB::ProxyConfiguration{});
    }

    /// The reset/expired reason counters are only filled in for the DISK group.
    DB::IHTTPConnectionPoolForEndpoint::Ptr getDiskPool()
    {
        auto uri = Poco::URI(getServerUrl());
        return DB::HTTPConnectionPools::instance().getPool(DB::HTTPConnectionGroupType::DISK, uri, DB::ProxyConfiguration{});
    }

    std::string getServerUrl() const
    {
        return "http://" + server_data.server->socket().address().toString();
    }

    void startServer()
    {
        server_data.reset();
        server_data.handler_factory = new HTTPRequestHandlerFactory(options);
        server_data.server =  std::make_unique<Poco::Net::HTTPServer>(
            server_data.handler_factory, server_data.port);

        server_data.server->start();
    }

    Poco::Net::HTTPServer & getServer() const
    {
        return *server_data.server;
    }

    void setSlowDown(size_t seconds)
    {
        auto opt = options->get();
        opt.slowdown_receive = seconds;
        options->set(std::move(opt));
    }

    void setOverWriteKeepAlive(size_t seconds, int max_requests)
    {
        auto opt = options->get();
        opt.overwrite_keep_alive_timeout = int(seconds);
        opt.overwrite_keep_alive_max_requests= max_requests;
        options->set(std::move(opt));
    }

    void setResponseNoKeepAlive(bool value)
    {
        auto opt = options->get();
        opt.response_no_keep_alive = value;
        options->set(std::move(opt));
    }

    void setTrailingGarbageBytes(size_t value)
    {
        auto opt = options->get();
        opt.trailing_garbage_bytes = value;
        options->set(std::move(opt));
    }

    DB::ConnectionTimeouts timeouts;
    SafeHandler<RequestOptions>::Ptr options;

    struct ServerData
    {
        // just some port to avoid collisions with others tests
        UInt16 port = 9871;

        HTTPRequestHandlerFactory::Ptr handler_factory;
        std::unique_ptr<Poco::Net::HTTPServer> server;

        ServerData() = default;
        ServerData(ServerData &&) = default;
        ServerData & operator =(ServerData &&) = delete;

        void reset()
        {
            if (server)
                server->stop();

            server = nullptr;
            handler_factory = nullptr;
        }

        ~ServerData() {
            reset();
        }
    };

    ServerData server_data;
};


static void wait_until(std::function<bool()> pred)
{
    while (!pred())
        sleepForMilliseconds(10);
}

static void echoRequest(String data, HTTPSession & session)
{
    {
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", "HTTP/1.1"); // HTTP/1.1 is required for keep alive
        request.setContentLength(data.size());
        std::ostream & ostream = session.sendRequest(request);
        ostream << data;
    }

    {
        std::stringstream result;
        Poco::Net::HTTPResponse response;
        std::istream & istream = session.receiveResponse(response);
        ASSERT_EQ(response.getStatus(), Poco::Net::HTTPResponse::HTTP_OK);

        stream_copy_n(istream, result);
        ASSERT_EQ(data, result.str());
    }
}

TEST_F(ConnectionPoolTest, CanConnect)
{
    auto pool = getPool();
    auto connection = pool->getConnection(timeouts, nullptr);

    ASSERT_TRUE(connection->connected());
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);

    ASSERT_EQ(1, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    (*connection).reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[pool->getMetrics().created]);
}

TEST_F(ConnectionPoolTest, CanRequest)
{
    auto pool = getPool();
    auto connection = pool->getConnection(timeouts, nullptr);

    echoRequest("Hello", *connection);

    ASSERT_EQ(1, getServer().totalConnections());
    ASSERT_EQ(1, getServer().currentConnections());

    (*connection).reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    auto metrics = pool->getMetrics();

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, CanPreserve)
{
    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));

    wait_until([&] () { return getServer().currentConnections() == 1; });
    ASSERT_EQ(1, getServer().currentConnections());
}

TEST_F(ConnectionPoolTest, CanReuse)
{
    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
    }

    {
        auto connection = pool->getConnection(timeouts, nullptr);

        ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
        ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));

        wait_until([&] () { return getServer().currentConnections() == 1; });
        ASSERT_EQ(1, getServer().currentConnections());

        echoRequest("Hello", *connection);

        ASSERT_EQ(1, getServer().totalConnections());
        ASSERT_EQ(1, getServer().currentConnections());

        ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
        ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
        ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reused]);
        ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);

        (*connection).reset();
    }

    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().active_count));
    ASSERT_EQ(0, CurrentMetrics::get(pool->getMetrics().stored_count));

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
}

TEST_F(ConnectionPoolTest, CanReuse10)
{
    auto pool = getPool();
    auto metrics = pool->getMetrics();

    for (int i = 0; i < 10; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        (*connection).reset(); // reset just not to wait its expiration here
    }

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(1, getServer().totalConnections());


    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, CanReuse5)
{
    auto ka = Poco::Timespan(1, 0); // 1 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    std::vector<DB::HTTPSessionPtr> connections;
    connections.reserve(5);
    for (int i = 0; i < 5; ++i)
    {
        connections.push_back(pool->getConnection(timeouts, nullptr));
    }
    connections.clear();

    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(5, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(5, CurrentMetrics::get(metrics.stored_count));

    wait_until([&] () { return getServer().currentConnections() == 5; });
    ASSERT_EQ(5, getServer().currentConnections());
    ASSERT_EQ(5, getServer().totalConnections());

    for (int i = 0; i < 5; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }

    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(5, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(5, CurrentMetrics::get(metrics.stored_count));

    /// wait until all connections are timeouted
    wait_until([&] () { return getServer().currentConnections() == 0; });

    {
        // just to trigger pool->wipeExpired();
        auto connection = pool->getConnection(timeouts, nullptr);
        (*connection).reset();
    }

    ASSERT_EQ(6, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, CanReconnectAndCreate)
{
    auto pool = getPool();
    auto metrics = pool->getMetrics();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 3;
    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        in_use.push_back(connection);
    }

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(count, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));

    auto connection = std::move(in_use.back());
    in_use.pop_back();

    echoRequest("Hello", *connection);

    connection->abort(); // further usage requires reconnect, new connection

    echoRequest("Hello", *connection);

    ASSERT_EQ(count+1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(count, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, CanReconnectAndReuse)
{
    auto ka = Poco::Timespan(1, 0); // 1 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    std::vector<HTTPSessionPtr> in_use;

    const size_t count = 3;
    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        /// make some request in order to show to the server the keep alive headers
        echoRequest("Hello", *connection);
        in_use.push_back(std::move(connection));
    }
    in_use.clear();

    for (int i = 0; i < count; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        in_use.push_back(std::move(connection));
    }

    auto connection = std::move(in_use.back());
    in_use.pop_back();
    in_use.clear(); // other connection will be reused

    echoRequest("Hello", *connection);

    connection->abort(); // further usage requires reconnect, reuse connection from pool

    echoRequest("Hello", *connection);

    (*connection).reset();

    wait_until([&] () { return getServer().currentConnections() == 0; });
    ASSERT_EQ(0, getServer().currentConnections());
    ASSERT_EQ(count, getServer().totalConnections());

    ASSERT_EQ(count, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(count + count - 1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(count + 1, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(count-1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(count-2, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, ReceiveTimeout)
{
    setSlowDown(2);
    timeouts.withReceiveTimeout(1);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        ASSERT_ANY_THROW(
            echoRequest("Hello", *connection);
        );
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));

    {
        timeouts.withReceiveTimeout(3);
        auto connection = pool->getConnection(timeouts, nullptr);
        ASSERT_NO_THROW(
            echoRequest("Hello", *connection);
        );
    }

    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));

    {
        /// timeouts have effect for reused session
        timeouts.withReceiveTimeout(1);
        auto connection = pool->getConnection(timeouts, nullptr);
        ASSERT_ANY_THROW(
            echoRequest("Hello", *connection);
        );
    }

    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, ReadWriteBufferFromHTTP)
{
    std::string_view message = "Hello ReadWriteBufferFromHTTP";
    auto uri = Poco::URI(getServerUrl());
    auto metrics = DB::HTTPConnectionPools::instance().getPool(DB::HTTPConnectionGroupType::HTTP, uri, DB::ProxyConfiguration{})->getMetrics();

    Poco::Net::HTTPBasicCredentials empty_creds;
    auto buf_from_http = DB::BuilderRWBufferFromHTTP(uri)
                             .withBypassProxy(true)
                             .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
                             .withOutCallback(
                                 [&] (std::ostream & in)
                                 {
                                     in << message;
                                 })
                             .withDelayInit(false)
                             .create(empty_creds);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));

    char buf[256];
    std::fill(buf, buf + sizeof(buf), 0);

    buf_from_http->readStrict(buf, message.size());
    ASSERT_EQ(std::string_view(buf), message);
    ASSERT_TRUE(buf_from_http->eof());

    buf_from_http.reset();

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, StoreLimit)
{
    DB::HTTPConnectionPools::Limits zero_limits {0, 0, 0};
    DB::HTTPConnectionPools::instance().setLimits(zero_limits, zero_limits, zero_limits);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, HardLimit)
{
    DB::HTTPConnectionPools::Limits limits {0, 0, 1, 1};
    DB::HTTPConnectionPools::instance().setLimits(limits, limits, limits);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection1 = pool->getConnection(timeouts, nullptr);
        ASSERT_ANY_THROW(pool->getConnection(timeouts, nullptr));
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, NoReceiveCall)
{
    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);

        {
            auto data = String("Hello");
            Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", "HTTP/1.1"); // HTTP/1.1 is required for keep alive
            request.setContentLength(data.size());
            std::ostream & ostream = connection->sendRequest(request);
            ostream << data;
        }

        connection->flushRequest();
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, ReconnectedWhenConnectionIsHoldTooLong)
{
    auto ka = Poco::Timespan(1, 0); // 1 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);

        echoRequest("Hello", *connection);

        auto fake_ka = Poco::Timespan(30 * 1000 * 1000); // 30 seconds
        timeouts.withHTTPKeepAliveTimeout(fake_ka);
        DB::setTimeouts(*connection, timeouts); // new keep alive timeout has no effect

        wait_until([&] () { return getServer().currentConnections() == 0; });

        ASSERT_EQ(1, connection->connected());
        ASSERT_EQ(1, connection->getKeepAlive());
        ASSERT_EQ(1000, connection->getKeepAliveTimeout().totalMilliseconds());
        ASSERT_EQ(1, connection->isKeepAliveExpired(connection->getKeepAliveReliability()));

        echoRequest("Hello", *connection);
    }


    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, ReconnectedWhenConnectionIsNearlyExpired)
{
    auto ka = Poco::Timespan(1, 0); // 1 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        {
            auto connection = pool->getConnection(timeouts, nullptr);
            echoRequest("Hello", *connection);
        }

        sleepForMilliseconds(900);

        {
            auto connection = pool->getConnection(timeouts, nullptr);
            echoRequest("Hello", *connection);
        }
    }

    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, ServerOverwriteKeepAlive)
{
    auto ka = Poco::Timespan(30, 0); // 30 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(30, timeouts.http_keep_alive_timeout.totalSeconds());
        ASSERT_EQ(30, connection->getKeepAliveTimeout().totalSeconds());
    }

    {
        setOverWriteKeepAlive(1, 10);
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(30, timeouts.http_keep_alive_timeout.totalSeconds());
        ASSERT_EQ(1, connection->getKeepAliveTimeout().totalSeconds());
    }

    {
        // server do not overwrite it in the following requests but client has to remember last agreed value
        setOverWriteKeepAlive(0, 0);
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(30, timeouts.http_keep_alive_timeout.totalSeconds());
        ASSERT_EQ(1, connection->getKeepAliveTimeout().totalSeconds());
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(3, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));
}

TEST_F(ConnectionPoolTest, MaxRequests)
{
    auto ka = Poco::Timespan(30, 0); // 30 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);
    auto max_requests = 5;
    timeouts.http_keep_alive_max_requests = max_requests;

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    for (int i = 1; i <= max_requests - 1; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(30, connection->getKeepAliveTimeout().totalSeconds());
        ASSERT_EQ(max_requests, connection->getKeepAliveMaxRequests());
        ASSERT_EQ(i, connection->getKeepAliveRequest());
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(max_requests-1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(max_requests-2, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(1, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(1, CurrentMetrics::get(metrics.stored_count));

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(30, connection->getKeepAliveTimeout().totalSeconds());
        ASSERT_EQ(max_requests, connection->getKeepAliveMaxRequests());
        ASSERT_EQ(max_requests, connection->getKeepAliveRequest());
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(max_requests-1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(max_requests-1, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}


TEST_F(ConnectionPoolTest, ServerOverwriteMaxRequests)
{
    auto ka = Poco::Timespan(30, 0); // 30 seconds
    timeouts.withHTTPKeepAliveTimeout(ka);

    auto pool = getPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(30, connection->getKeepAliveTimeout().totalSeconds());
        ASSERT_EQ(1000, connection->getKeepAliveMaxRequests());
        ASSERT_EQ(1, connection->getKeepAliveRequest());
    }

    auto max_requests = 3;
    setOverWriteKeepAlive(5, max_requests);

    for (int i = 2; i <= 10*max_requests; ++i)
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        ASSERT_EQ(5, connection->getKeepAliveTimeout().totalSeconds());
        ASSERT_EQ(max_requests, connection->getKeepAliveMaxRequests());
        ASSERT_EQ(((i-1) % max_requests) + 1, connection->getKeepAliveRequest());
    }

    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[metrics.created]);
    ASSERT_EQ(10*max_requests-10, DB::CurrentThread::getProfileEvents()[metrics.preserved]);
    ASSERT_EQ(10*max_requests-10, DB::CurrentThread::getProfileEvents()[metrics.reused]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(10, DB::CurrentThread::getProfileEvents()[metrics.expired]);

    ASSERT_EQ(0, CurrentMetrics::get(metrics.active_count));
    ASSERT_EQ(0, CurrentMetrics::get(metrics.stored_count));
}

/// Covers every branch of atConnectionDestroy() and wipeExpiredImpl() except
/// reset_preserve_exception: reset_disconnected and expired_stale_peer need the server to sever
/// the socket ahead of the negotiated keep-alive, which HTTPServer::stopAll(true) makes possible
/// (see ResetDisconnectedReasonIsCounted and ExpiredStalePeerReasonIsCounted below), but once
/// that is used the server is gone for the rest of the test, so those two are kept in their own
/// tests; reset_unread_buffered_data needs the mock server to append raw bytes after a complete
/// response (see ResetUnreadBufferedDataReasonIsCounted below), also kept separate since it
/// needs its own mock-handler wiring. reset_preserve_exception needs the try block's
/// PooledConnection::create (or assign/notifySocketInode/the stored_connections push) to throw;
/// the only throw site controllable from a test is atConnectionCreate's hard-limit check, but
/// atConnectionDestroy always decrements total_connections_in_group for the connection being
/// destroyed *before* attempting to create its storage wrapper, so that check can never see the
/// group back at the hard limit at the moment it runs (confirmed empirically with hard_limit 1
/// and 2, one and two connections held: both cases end up preserved, reset staying 0). This
/// fixture has no lever for reset_preserve_exception, so it is not covered.
TEST_F(ConnectionPoolTest, ResetAndExpiredReasonsAreCounted)
{
    auto ka = Poco::Timespan(1, 0); // 1 second
    timeouts.withHTTPKeepAliveTimeout(ka);
    auto pool = getDiskPool();
    auto metrics = pool->getMetrics();

    /// Keep-alive age: destroy a connection after holding it past 0.9 x keep-alive, with no
    /// second request in between. A second request would hit sendRequest()'s own preamble
    /// (connected() && !keepAlive) || mustReconnect(), which transparently reconnects and
    /// tears down the *old* socket as already disconnected (verified: that path reports
    /// reset_disconnected, not reset_keep_alive_age) before atConnectionDestroy ever sees it;
    /// only a connection that is simply let go once it is too old exercises this reason.
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
        sleepForMilliseconds(950);
    }
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_keep_alive_age]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset_disconnected]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset_response_not_keep_alive]);

    /// Stored connection older than 0.8 x keep-alive: expired by age at the next wipe.
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    sleepForMilliseconds(900);
    {
        /// wipeExpiredImpl() runs at the top of getConnection(), so a throwaway connection
        /// is enough to trigger it; .reset() keeps this one out of `preserved`, at the cost
        /// of adding one more reset(disconnected) of its own (accounted for below).
        auto connection = pool->getConnection(timeouts, nullptr);
        (*connection).reset();
    }
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired_age]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired_stale_peer]);
    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_disconnected]);

    /// Max requests: the first request of a max_requests=1 session already returns as expired(max_requests).
    timeouts.withHTTPKeepAliveMaxRequests(1);
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    ASSERT_EQ(2, DB::CurrentThread::getProfileEvents()[metrics.expired]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired_max_requests]);

    /// Back to the plain 1-second keep-alive, unlimited requests, for the remaining blocks.
    timeouts = DB::ConnectionTimeouts();
    timeouts.withHTTPKeepAliveTimeout(ka);

    /// Response not keep-alive: the server sends Connection: close. isKeepAliveExpired() is
    /// ruled out first (the request is immediate), so the residual mustReconnect() is counted
    /// as the response header, not as age.
    setResponseNoKeepAlive(true);
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    setResponseNoKeepAlive(false);
    ASSERT_EQ(3, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_response_not_keep_alive]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_keep_alive_age]);

    /// Store limit: a connection that would otherwise be preserved is dropped instead, same as
    /// the plain StoreLimit test above, but observed through the disk pool's reason counter.
    DB::HTTPConnectionPools::Limits zero_limits {0, 0, 0};
    DB::HTTPConnectionPools::instance().setLimits(zero_limits, zero_limits, zero_limits);
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    DB::HTTPConnectionPools::Limits def_limits {};
    DB::HTTPConnectionPools::instance().setLimits(def_limits, def_limits, def_limits);
    ASSERT_EQ(4, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_store_limit]);

    /// Incomplete request/response: sendRequest() marks the response as not-yet-complete;
    /// with receiveResponse() never called, the connection is torn down before it can be
    /// preserved, same as the plain NoReceiveCall test above.
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        auto data = String("Hello");
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, "/", "HTTP/1.1");
        request.setContentLength(data.size());
        std::ostream & ostream = connection->sendRequest(request);
        ostream << data;
        connection->flushRequest();
    }
    ASSERT_EQ(5, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_incomplete_request_or_response]);
}

TEST_F(ConnectionPoolTest, ResetDisconnectedReasonIsCounted)
{
    /// A long keep-alive so age never trips; only the forced peer-side shutdown should.
    auto ka = Poco::Timespan(30, 0);
    timeouts.withHTTPKeepAliveTimeout(ka);
    auto pool = getDiskPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);

        /// Force an immediate peer-side shutdown regardless of the negotiated keep-alive:
        /// HTTPServer::stopAll(true) shuts down every accepted socket right away (unlike
        /// stop(), which only stops accepting new connections and leaves live ones alone).
        getServer().stopAll(true);
        wait_until([&] () { return getServer().currentConnections() == 0; });

        /// The next attempt on the now-severed socket fails; Poco's receiveResponse() (or, if
        /// the write itself fails, the reconnect a failed write triggers, since the pool is
        /// gone too) closes the session before rethrowing, so connected() is already false by
        /// the time this connection is destroyed.
        ASSERT_ANY_THROW(echoRequest("Hello", *connection));
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_disconnected]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset_keep_alive_age]);
}

TEST_F(ConnectionPoolTest, ExpiredStalePeerReasonIsCounted)
{
    /// A long keep-alive so age never trips before the forced peer-side shutdown does.
    auto ka = Poco::Timespan(30, 0);
    timeouts.withHTTPKeepAliveTimeout(ka);
    auto pool = getDiskPool();
    auto metrics = pool->getMetrics();

    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.preserved]);

    /// Force an immediate peer-side shutdown of the now-idle, stored connection; isStale()'s
    /// poll(SELECT_READ) sees the pending FIN well before the 0.8 x keep-alive age threshold
    /// (24 seconds away here) has any chance to fire.
    getServer().stopAll(true);
    wait_until([&] () { return getServer().currentConnections() == 0; });

    /// wipeExpiredImpl() runs at the top of getConnection(), so a throwaway connection is
    /// enough to trigger it; the listener is stopped too, so the fresh connect this then
    /// attempts (the stored connection was just wiped) is expected to fail.
    try
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        (*connection).reset();
    }
    catch (...)
    {
    }

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.expired_stale_peer]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.expired_age]);
}

TEST_F(ConnectionPoolTest, ResetUnreadBufferedDataReasonIsCounted)
{
    auto pool = getDiskPool();
    auto metrics = pool->getMetrics();

    /// The server appends bytes after a complete, correctly-framed response, all as one raw
    /// write (see trailing_garbage_bytes in the mock handler above). Poco's HTTPSession reads
    /// ahead in HTTP_DEFAULT_BUFFER_SIZE (8 KiB) chunks regardless of how few bytes the caller
    /// ultimately wants, so on a small response these trailing bytes end up sitting in the
    /// client's internal buffer once the caller has consumed exactly Content-Length bytes:
    /// isCompleted() is true (the declared response was received in full) but buffered() is
    /// still nonzero.
    setTrailingGarbageBytes(16);
    {
        auto connection = pool->getConnection(timeouts, nullptr);
        echoRequest("Hello", *connection);
    }
    setTrailingGarbageBytes(0);

    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset]);
    ASSERT_EQ(1, DB::CurrentThread::getProfileEvents()[metrics.reset_unread_buffered_data]);
    ASSERT_EQ(0, DB::CurrentThread::getProfileEvents()[metrics.reset_incomplete_request_or_response]);
}
