#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
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
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    Poco::Net::MessageHeader last_request_header;

public:
    TestPocoHTTPServer():
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new HTTPRequestHandlerFactory(last_request_header)),
        server_params(new Poco::Net::HTTPServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_header;
    }
};

/// Fails the first `fail_first_n` requests with `fail_status` (empty body), then serves `body` with a
/// 200 to every request after. Records every request's header (not just the last) so a caller can
/// check the sequence a local retry produced.
class SequenceRecordingRequestHandler : public Poco::Net::HTTPRequestHandler
{
    std::vector<Poco::Net::MessageHeader> & all_request_headers;
    size_t & requests_seen;
    size_t fail_first_n;
    Poco::Net::HTTPResponse::HTTPStatus fail_status;
    std::string body;

public:
    SequenceRecordingRequestHandler(
        std::vector<Poco::Net::MessageHeader> & all_request_headers_,
        size_t & requests_seen_,
        size_t fail_first_n_,
        Poco::Net::HTTPResponse::HTTPStatus fail_status_,
        std::string body_)
        : all_request_headers(all_request_headers_)
        , requests_seen(requests_seen_)
        , fail_first_n(fail_first_n_)
        , fail_status(fail_status_)
        , body(std::move(body_))
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        all_request_headers.push_back(request);
        ++requests_seen;

        if (requests_seen <= fail_first_n)
        {
            response.setStatus(fail_status);
            response.send();
            return;
        }

        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        response.setContentLength(static_cast<std::streamsize>(body.size()));
        auto & out = response.send();
        out << body;
        out.flush();
    }
};

class SequenceRecordingRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    std::vector<Poco::Net::MessageHeader> & all_request_headers;
    size_t & requests_seen;
    size_t fail_first_n;
    Poco::Net::HTTPResponse::HTTPStatus fail_status;
    std::string body;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new SequenceRecordingRequestHandler(all_request_headers, requests_seen, fail_first_n, fail_status, body);
    }

public:
    SequenceRecordingRequestHandlerFactory(
        std::vector<Poco::Net::MessageHeader> & all_request_headers_,
        size_t & requests_seen_,
        size_t fail_first_n_,
        Poco::Net::HTTPResponse::HTTPStatus fail_status_,
        std::string body_)
        : all_request_headers(all_request_headers_)
        , requests_seen(requests_seen_)
        , fail_first_n(fail_first_n_)
        , fail_status(fail_status_)
        , body(std::move(body_))
    {
    }

    ~SequenceRecordingRequestHandlerFactory() override = default;
};

/// Like `TestPocoHTTPServer`, but for driving a real local retry: the first `fail_first_n` requests
/// get `fail_status`, every one after gets `body` with a 200, and every request's header is kept (not
/// just the last).
class TestPocoHTTPSequenceServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<SequenceRecordingRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    std::vector<Poco::Net::MessageHeader> all_request_headers;
    size_t requests_seen = 0;

public:
    TestPocoHTTPSequenceServer(size_t fail_first_n, Poco::Net::HTTPResponse::HTTPStatus fail_status, std::string body = {}):
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new SequenceRecordingRequestHandlerFactory(all_request_headers, requests_seen, fail_first_n, fail_status, std::move(body))),
        server_params(new Poco::Net::HTTPServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    const std::vector<Poco::Net::MessageHeader> & getAllRequestHeaders() const
    {
        return all_request_headers;
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
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    std::optional<StsRequestInfo> last_request_info;

public:
    TestPocoHTTPStsServer(std::string role_access_key, std::string role_secret_key):
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new StsHTTPRequestHandlerFactory(last_request_info, std::move(role_access_key), std::move(role_secret_key))),
        server_params(new Poco::Net::HTTPServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
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
