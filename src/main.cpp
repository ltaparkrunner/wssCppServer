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

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <mongocxx/instance.hpp>

#include "config.hpp"
#include "database.hpp"
#include "auth_utils.hpp"
#include "aux_handler.hpp"
#include "image.pb.h"
#include "auxil.hpp"
#include "wssSession.hpp"

namespace asio = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;

inline std::string url_decode(const std::string& in);
inline std::map<std::string, std::string> parse_form_body(const std::string& body);

class HttpServerSession : public std::enable_shared_from_this<HttpServerSession> {
    // ФИКС №1: Поменяли типы местами. tcp::socket теперь первый, у него есть дефолтный конструктор!
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
    // ФИКС №2: Явно вызываем init_stream в списке инициализации, чтобы variant не пытался создаться пустым
    HttpServerSession(tcp::socket socket, ssl::context& ctx, Database& db, Aws::S3::S3Client& s3, Config& cfg, bool is_auth)
        : stream_(init_stream(std::move(socket), ctx, is_auth)),
          db_(db), s3_client_(s3), cfg_(cfg), is_auth_server_(is_auth) {}

    void start() {
        // Проверяем по индексу 1, лежит ли там SSL-стрим (так как теперь ssl_stream стал вторым типом)
        if (std::holds_alternative<beast::ssl_stream<tcp::socket>>(stream_)) {
            std::get<beast::ssl_stream<tcp::socket>>(stream_).async_handshake(
                ssl::stream_base::server, 
                beast::bind_front_handler(&HttpServerSession::on_handshake, shared_from_this())
            );
        } else {
            do_read();
        }
    }

private:
    void on_handshake(beast::error_code ec) {
        if (ec) return;
        do_read();
    }

    void do_read() {
        req_ = {};
        // std::visit автоматически выберет нужный стрим/сокет для чтения
        std::visit([this](auto& active_stream) {
            http::async_read(active_stream, buffer_, req_, 
                beast::bind_front_handler(&HttpServerSession::on_read, shared_from_this()));
        }, stream_);
    }

    void on_read(beast::error_code ec, std::size_t) {
        // Проверяем ошибки чтения ДО логирования (фикс пустых строк в логе)
        if (ec == http::error::end_of_stream) {
            std::cerr << "HTTP Connection closed by client." << std::endl;
            return;
        }
        if (ec) { 
            std::cerr << "HTTP Read Error: " << ec.message() << std::endl;
            return;
        }

        // Логируем только РЕАЛЬНО скачанный HTTP запрос
        std::cout << "Received HTTP request: " << req_.method_string() << " " << req_.target() << std::endl;

        // Обработка WebSocket апгрейда (только на основном порту 8080)
        if (!is_auth_server_ && websocket::is_upgrade(req_)) {
            std::string auth_header = std::string(req_[http::field::authorization]);
            if (auth_header.empty() || auth_header.rfind("Bearer ", 0) != 0) {
                send_http_string_response(http::status::unauthorized, "Unauthorized");
                return;
            }
            
            std::string token = auth_header.substr(7);
            try {
                auto decoded = jwt::decode<jwt::traits::nlohmann_json>(token);
                std::string user_id = decoded.get_payload_claim("id").as_string();

                // Так как это порт 8080, внутри гарантированно лежит ssl_stream
                auto& ssl_stream = std::get<beast::ssl_stream<tcp::socket>>(stream_);

                auto wss = std::make_shared<WssSession>(
                    std::move(ssl_stream), 
                    db_, s3_client_, cfg_, user_id
                );
                
                wss->start(std::move(req_));
                return;
            } catch (...) {
                send_http_string_response(http::status::unauthorized, "Unauthorized");
                return;
            }
        }

        if (is_auth_server_) {
            handle_rest_api();
        } else {
            send_json_response(http::status::not_found, json{{"error", "Not Found"}});
        }
    }

    void handle_rest_api() {
        std::string content_type = std::string(req_[http::field::content_type]);
        std::string username, password;

        // Эмуляция Node.js (express.json / urlencoded) — парсим данные из любой структуры
        if (content_type.find("application/x-www-form-urlencoded") != std::string::npos) {
            auto fields = parse_form_body(req_.body());
            username = fields["username"];
            password = fields["password"];
        } else {
            try {
                auto body = json::parse(req_.body());
                username = body.value("username", "");
                password = body.value("password", "");
            } catch (...) {
                // Если прилетел невалидный JSON
            }
        }

        if (req_.method() == http::verb::post && req_.target() == "/auth/register") {
            std::cout << "Received registration request: username=" << username << std::endl;
            try {
                if (username.empty() || password.empty()) {
                    send_json_response(http::status::bad_request, json{{"error", "Missing fields"}});
                    return;
                }

                auto collection = db_.get_db()["users"];
                auto existing = collection.find_one(bsoncxx::builder::stream::document{} << "login" << username << bsoncxx::builder::stream::finalize);
                
                if (existing) {
                    send_json_response(http::status::bad_request, json{{"error", "Пользователь уже существует"}});
                    return;
                }

                std::string pass_hash = AuthUtils::hash_password(password);
                auto res = collection.insert_one(bsoncxx::builder::stream::document{} << "login" << username << "password" << pass_hash << bsoncxx::builder::stream::finalize);
                
                std::string uid = res->inserted_id().get_oid().value.to_string();
                std::string token = AuthUtils::generate_token(uid, cfg_.jwt_secret);

                send_json_response(http::status::created, json{{"message", "Пользователь создан"}, {"token", token}});
            } catch (...) {
                send_json_response(http::status::internal_server_error, json{{"error", "Ошибка регистрации"}});
            }
        }
    }

    void send_json_response(http::status status, const json& body) {
        auto res = std::make_shared<http::response<http::string_body>>(status, req_.version());
        res->set(http::field::content_type, "application/json");
        res->body() = body.dump();
        res->keep_alive(req_.keep_alive()); // Передаем состояние Keep-Alive клиенту
        res->prepare_payload();

        std::visit([this, res](auto& active_stream) {
            http::async_write(active_stream, *res, [self = shared_from_this(), res](beast::error_code ec, std::size_t) {
                // Фикс KEEP-ALIVE (как в Node.js): взводим чтение следующего запроса в этом же сокете
                if (!ec && res->keep_alive()) {
                    self->do_read();
                }
            });
        }, stream_);
    }

    void send_http_string_response(http::status status, const std::string& msg) {
        auto res = std::make_shared<http::response<http::string_body>>(status, req_.version());
        res->set(http::field::content_type, "text/plain");
        res->body() = msg;
        res->keep_alive(req_.keep_alive());
        res->prepare_payload();

        std::visit([this, res](auto& active_stream) {
            http::async_write(active_stream, *res, [self = shared_from_this(), res](beast::error_code ec, std::size_t) {
                if (!ec && res->keep_alive()) {
                    self->do_read();
                }
            });
        }, stream_);
    }
};

// Сетевой слушатель портов
class NetworkListener : public std::enable_shared_from_this<NetworkListener> {
    asio::io_context& ioc_;
    ssl::context& ctx_;
    tcp::acceptor acceptor_;
    Database& db_;
    Aws::S3::S3Client& s3_client_;
    Config& cfg_;
    bool is_auth_;

public:
    NetworkListener(asio::io_context& ioc, ssl::context& ctx, tcp::endpoint ep, Database& db, Aws::S3::S3Client& s3, Config& cfg, bool is_auth)
        : ioc_(ioc), ctx_(ctx), acceptor_(asio::make_strand(ioc)), db_(db), s3_client_(s3), cfg_(cfg), is_auth_(is_auth) {
        acceptor_.open(ep.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
    }

    void start() { do_accept(); }

private:
    void do_accept() {
        acceptor_.async_accept(asio::make_strand(ioc_), beast::bind_front_handler(&NetworkListener::on_accept, shared_from_this()));
    }

    void on_accept(beast::error_code ec, tcp::socket socket) {
        if (!ec) {
            std::make_shared<HttpServerSession>(std::move(socket), ctx_, db_, s3_client_, cfg_, is_auth_)->start();
        }
        do_accept();
    }
};

int main() {
    mongocxx::instance instance{}; 
    auto cfg = Config::load();
    Database db(cfg.mongo_url);

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    Aws::S3::S3Client s3_client;

    const int threads_count = std::max(1u, std::thread::hardware_concurrency());
    asio::io_context ioc{threads_count};

    ssl::context ctx{ssl::context::tlsv12_server};
    ctx.use_certificate_chain_file("cert.pem");
    ctx.use_private_key_file("key.pem", ssl::context::pem);

    auto wss_ep = tcp::endpoint{asio::ip::make_address("0.0.0.0"), 8080};
    auto auth_ep = tcp::endpoint{asio::ip::make_address("0.0.0.0"), 8081};

    std::make_shared<NetworkListener>(ioc, ctx, wss_ep, db, s3_client, cfg, false)->start();
    std::make_shared<NetworkListener>(ioc, ctx, auth_ep, db, s3_client, cfg, true)->start();

    std::vector<std::thread> threads;
    for (int i = 0; i < threads_count - 1; ++i) {
        threads.emplace_back([&ioc]() { ioc.run(); });
    }
    std::cout << "C++ Servers started. WSS on 8080, Auth REST on 8081." << std::endl;
    ioc.run();

    Aws::ShutdownAPI(options);
    return 0;
}
