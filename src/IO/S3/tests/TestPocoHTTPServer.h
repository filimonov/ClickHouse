#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/URI.h>
#include <Poco/AutoPtr.h>
#include <Poco/SharedPtr.h>
#include <Poco/ThreadPool.h>
#include <fmt/format.h>

class MockRequestHandler : public Poco::Net::HTTPRequestHandler
{
    Poco::Net::MessageHeader & last_request_header;

public:
    explicit MockRequestHandler(Poco::Net::MessageHeader & last_request_header_)
    : last_request_header(last_request_header_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        last_request_header = request;
        response.send();
    }
};

class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    Poco::Net::MessageHeader & last_request_header;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandler(last_request_header);
    }

public:
    explicit HTTPRequestHandlerFactory(Poco::Net::MessageHeader & last_request_header_)
    : last_request_header(last_request_header_)
    {
    }

    ~HTTPRequestHandlerFactory() override = default;
};

class TestPocoHTTPServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<HTTPRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    /// A dedicated pool instead of `Poco::ThreadPool::defaultPool()` (what the `HTTPServer`
    /// constructor uses when none is given explicitly): `TCPServerDispatcher::enqueue`
    /// (`base/poco/Net/src/TCPServerDispatcher.cpp`) has an acknowledged-in-comment bug where its
    /// saturation check races once more than one `TCPServerDispatcher` shares that pool, so a
    /// connection can be accepted and then immediately closed with no response whenever this
    /// binary's OTHER local-server tests have the shared pool saturated at that moment. A private
    /// pool makes this server's thread accounting exact regardless of how many other in-process
    /// servers this binary runs.
    Poco::ThreadPool thread_pool;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    Poco::Net::MessageHeader last_request_header;

public:
    TestPocoHTTPServer():
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new HTTPRequestHandlerFactory(last_request_header)),
        server_params(new Poco::Net::HTTPServerParams()),
        thread_pool("TestPocoHTTPServer"),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, thread_pool, *server_socket, server_params))
    {
        server->start();
    }

    /// `~HTTPServer()`'s own `stop()` (via `TCPServer::stop()`) lets an active connection finish at
    /// its own pace -- with the private `thread_pool` above, its worker thread would otherwise still
    /// be blocked reading for a next request when `thread_pool`'s destructor tries to join it.
    /// `stopAll(true)` aborts active connections immediately (shuts down their sockets), so the
    /// worker returns right away and `thread_pool.joinAll()` below has nothing left to wait for. Runs
    /// while every member this server's handler touches is still alive: this is destructor BODY code,
    /// executed before any member's own destructor begins.
    ~TestPocoHTTPServer()
    {
        server->stopAll(true);
        thread_pool.joinAll();
    }

    /// `server_socket->address()` is the wildcard bind address (`0.0.0.0:PORT`), which is not a usable
    /// connection target. Build the URL from an explicit loopback address plus the bound port instead.
    std::string getUrl()
    {
        return "http://127.0.0.1:" + std::to_string(server_socket->address().port());
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_header;
    }
};

struct StsRequestInfo
{
    Poco::Net::MessageHeader headers;
    Poco::URI::QueryParameters query_params;
};

class MockStsRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    explicit MockStsRequestHandler(std::optional<StsRequestInfo> & last_request_info_, std::string role_access_key_, std::string role_secret_key_)
        : last_request_info(last_request_info_)
        , role_access_key(std::move(role_access_key_))
        , role_secret_key(std::move(role_secret_key_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        last_request_info.emplace();
        last_request_info->headers = request;

        Poco::URI uri(request.getURI());
        last_request_info->query_params = uri.getQueryParameters();

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        auto & out = response.send();

        std::string result_xml = fmt::format(R"(
<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
<AssumeRoleResult>
    <Credentials>
        <AccessKeyId>{}</AccessKeyId>
        <SecretAccessKey>{}</SecretAccessKey>
        <SessionToken>session_token</SessionToken>
    </Credentials>
</AssumeRoleResult>
</AssumeRoleResponse>)", role_access_key, role_secret_key);
        out << result_xml;
        out.flush();
    }
private:
    std::optional<StsRequestInfo> & last_request_info;
    std::string role_access_key;
    std::string role_secret_key;
};

class StsHTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    std::optional<StsRequestInfo> & last_request_info;
    std::string role_access_key;
    std::string role_secret_key;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockStsRequestHandler(last_request_info, role_access_key, role_secret_key);
    }
public:
    explicit StsHTTPRequestHandlerFactory(std::optional<StsRequestInfo> & last_request_info_, std::string role_access_key_, std::string role_secret_key_)
        : last_request_info(last_request_info_)
        , role_access_key(std::move(role_access_key_))
        , role_secret_key(std::move(role_secret_key_))
    {
    }

    ~StsHTTPRequestHandlerFactory() override = default;
};

class TestPocoHTTPStsServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<StsHTTPRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    /// See the identical member in `TestPocoHTTPServer` above: a private pool avoids
    /// `TCPServerDispatcher`'s shared-pool saturation bug (base/poco/Net/src/TCPServerDispatcher.cpp).
    Poco::ThreadPool thread_pool;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    std::optional<StsRequestInfo> last_request_info;

public:
    TestPocoHTTPStsServer(std::string role_access_key, std::string role_secret_key):
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new StsHTTPRequestHandlerFactory(last_request_info, std::move(role_access_key), std::move(role_secret_key))),
        server_params(new Poco::Net::HTTPServerParams()),
        thread_pool("TestPocoHTTPStsServer"),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, thread_pool, *server_socket, server_params))
    {
        server->start();
    }

    /// See `TestPocoHTTPServer`'s destructor above: `stopAll(true)` aborts any active connection so
    /// its worker returns immediately, instead of `thread_pool`'s destructor having to join a thread
    /// still blocked reading for a next request.
    ~TestPocoHTTPStsServer()
    {
        server->stopAll(true);
        thread_pool.joinAll();
    }

    /// `server_socket->address()` is the wildcard bind address (`0.0.0.0:PORT`), which is not a usable
    /// connection target. Build the URL from an explicit loopback address plus the bound port instead.
    std::string getUrl()
    {
        return "http://127.0.0.1:" + std::to_string(server_socket->address().port());
    }

    void resetLastRequest()
    {
        last_request_info.reset();
    }

    bool hasLastRequest() const
    {
        return last_request_info.has_value();
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_info->headers;
    }

    const auto & getLastQueryParams() const
    {
        return last_request_info->query_params;
    }
};
