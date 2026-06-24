#pragma once
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <nlohmann/json.hpp>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <sstream>
#include <variant> // Обязательно для std::visit

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <jwt-cpp/jwt.h>
#include <jwt-cpp/traits/nlohmann-json/defaults.h> 

#include <bsoncxx/oid.hpp>
#include <mongocxx/instance.hpp>

#include "config.hpp"
#include "database.hpp"
#include "auth_utils.hpp"
#include "aux_handler.hpp"
#include "image.pb.h"
#include "wssSession.hpp"
//  #include "httpHandlers.hpp"


namespace asio = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;

class HttpServerSession : public std::enable_shared_from_this<HttpServerSession> {
    std::variant<tcp::socket, beast::ssl_stream<tcp::socket>> stream_;
    beast::flat_buffer buffer_;
    http::request<http::string_body> req_;
    Database& db_;
    Aws::S3::S3Client& s3_client_;
    Config& cfg_;
    bool is_auth_server_;

    // Вспомогательный статический метод для правильной инициализации std::variant в списке инициализации
    static std::variant<tcp::socket, beast::ssl_stream<tcp::socket>> init_stream(tcp::socket&& socket, ssl::context& ctx, bool is_auth) {
        if (is_auth) {
            return std::variant<tcp::socket, beast::ssl_stream<tcp::socket>>{std::in_place_index<0>, std::move(socket)};
        } else {
            return std::variant<tcp::socket, beast::ssl_stream<tcp::socket>>{std::in_place_index<1>, beast::ssl_stream<tcp::socket>(std::move(socket), ctx)};
        }
    }

public:
    HttpServerSession(tcp::socket socket, ssl::context& ctx, Database& db, Aws::S3::S3Client& s3, Config& cfg, bool is_auth);

    void start();

private:
    void on_handshake(beast::error_code ec) {
        if (ec) return;
        do_read();
    }

    void do_read();

    void on_read(beast::error_code ec, std::size_t);

    void handle_rest_api();

    void send_json_response(http::status status, const json& body);

    void send_http_string_response(http::status status, const std::string& msg);
};
