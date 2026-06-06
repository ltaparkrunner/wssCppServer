#include "httpServerSession.hpp" // Важно подключить заголовочный файл класса
#include "auxil.hpp"

HttpServerSession::HttpServerSession(tcp::socket socket, ssl::context& ctx, Database& db, Aws::S3::S3Client& s3, Config& cfg, bool is_auth)
: stream_(init_stream(std::move(socket), ctx, is_auth)),
  db_(db), s3_client_(s3), cfg_(cfg), is_auth_server_(is_auth) {}


void HttpServerSession::start() {
    if (std::holds_alternative<beast::ssl_stream<tcp::socket>>(stream_)) {
        std::get<beast::ssl_stream<tcp::socket>>(stream_).async_handshake(
            ssl::stream_base::server, 
            beast::bind_front_handler(&HttpServerSession::on_handshake, shared_from_this())
        );
    } else {
        do_read();
    }
}

void HttpServerSession::do_read() {
    req_ = {};
    // std::visit автоматически выберет нужный стрим/сокет для чтения
    std::visit([this](auto& active_stream) {
        http::async_read(active_stream, buffer_, req_, 
            beast::bind_front_handler(&HttpServerSession::on_read, shared_from_this()));
    }, stream_);
}

void HttpServerSession::on_read(beast::error_code ec, std::size_t) {
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

void HttpServerSession::handle_rest_api() {
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

void HttpServerSession::send_json_response(http::status status, const json& body) {
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

void HttpServerSession::send_http_string_response(http::status status, const std::string& msg) {
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