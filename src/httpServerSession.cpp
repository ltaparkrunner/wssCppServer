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
        
        if (username.empty() || password.empty()) {
            send_json_response(http::status::bad_request, json{{"error", "Missing fields"}});
            return;
        }
    
        auto self = shared_from_this();
        
        // ИСПРАВЛЕНО: Используем std::visit для безопасного извлечения executor из std::variant
        auto thread_pool_executor = std::visit([](auto& s) { 
            return beast::get_lowest_layer(s).get_executor(); 
        }, stream_);
        
        boost::asio::post(thread_pool_executor, [=, this]() {
            try {
                auto mongo_db = db_.get_db(); 
                auto users_collection = mongo_db["users"];
                auto records_collection = mongo_db["ImageRecord"];
                
                std::string bucket_name = cfg_.s3_bucket;
                std::string users_prefix = "USERS";
    
                auto existing = users_collection.find_one(
                    bsoncxx::builder::stream::document{} << "login" << username << bsoncxx::builder::stream::finalize
                );
                
                if (existing) {
                    // ИСПРАВЛЕНО: Тоже через std::visit возвращаем задачу в I/O поток сокета
                    auto io_executor = std::visit([](auto& s) { return beast::get_lowest_layer(s).get_executor(); }, stream_);
                    boost::asio::post(io_executor, [this, self]() {
                        send_json_response(http::status::bad_request, json{{"error", "Пользователь уже существует"}});
                    });
                    return;
                }
    
                Aws::S3::Model::HeadBucketRequest head_bucket_req;
                head_bucket_req.SetBucket(bucket_name);
                auto head_bucket_outcome = s3_client_.HeadBucket(head_bucket_req);
    
                if (!head_bucket_outcome.IsSuccess()) {
                    std::cout << "Bucket " << bucket_name << " does not exist. Creating..." << std::endl;
                    
                    // ИСПРАВЛЕНО: Теперь инклуд подключен, ошибка incomplete type исчезнет
                    Aws::S3::Model::CreateBucketRequest create_bucket_req;
                    create_bucket_req.SetBucket(bucket_name);
                    
                    auto create_bucket_outcome = s3_client_.CreateBucket(create_bucket_req);
                    if (!create_bucket_outcome.IsSuccess()) {
                        throw std::runtime_error("Failed to create S3 bucket: " + create_bucket_outcome.GetError().GetMessage());
                    }
                }
    
                std::string pass_hash = AuthUtils::hash_password(password);
                auto user_res = users_collection.insert_one(
                    bsoncxx::builder::stream::document{} 
                    << "login" << username 
                    << "password" << pass_hash 
                    << bsoncxx::builder::stream::finalize
                );
                
                if (!user_res) {
                    throw std::runtime_error("Failed to insert user into MongoDB");
                }
                
                std::string uid = user_res->inserted_id().get_oid().value.to_string();
                std::cout << "Before placeholder initialization for user id: " << uid << std::endl;
    
                std::string placeholder_path = users_prefix + "/" + uid + "/.placeholder";
                
                Aws::S3::Model::PutObjectRequest put_obj_req;
                put_obj_req.SetBucket(bucket_name);
                put_obj_req.SetKey(placeholder_path);
                
                auto request_stream = std::make_shared<std::stringstream>("");
                put_obj_req.SetBody(request_stream);
    
                auto put_obj_outcome = s3_client_.PutObject(put_obj_req);
                if (!put_obj_outcome.IsSuccess()) {
                    throw std::runtime_error("Failed to upload .placeholder to S3: " + put_obj_outcome.GetError().GetMessage());
                }
    
                using bsoncxx::builder::stream::open_document;
                using bsoncxx::builder::stream::close_document;
                
                auto placeholder_record = bsoncxx::builder::stream::document{}
                    << "name" << ".placeholder"
                    << "originalName" << ".placeholder"
                    << "folder" << (users_prefix + "/" + uid)
                    << "s3Key" << placeholder_path
                    << "bucket" << bucket_name
                    << "userLogin" << username
                    << "size" << 0
                    << "info" << open_document 
                        << "type" << "initialization_file" 
                    << close_document
                    << bsoncxx::builder::stream::finalize;
    
                records_collection.insert_one(placeholder_record.view());
    
                std::string token = AuthUtils::generate_token(uid, cfg_.jwt_secret);
    
                // ИСПРАВЛЕНО: Возвращаем отправку ответа через std::visit в I/O поток сокета
                auto io_executor = std::visit([](auto& s) { return beast::get_lowest_layer(s).get_executor(); }, stream_);
                boost::asio::post(io_executor, [this, self, token]() {
                    send_json_response(http::status::created, json{
                        {"message", "Пользователь создан"}, 
                        {"token", token}
                    });
                });
    
            } catch (const std::exception& e) {
                std::cerr << "Error during registration algorithm: " << e.what() << std::endl;
                auto io_executor = std::visit([](auto& s) { return beast::get_lowest_layer(s).get_executor(); }, stream_);
                boost::asio::post(io_executor, [this, self]() {
                    send_json_response(http::status::internal_server_error, json{{"error", "Ошибка сервера при регистрации"}});
                });
            } catch (...) {
                auto io_executor = std::visit([](auto& s) { return beast::get_lowest_layer(s).get_executor(); }, stream_);
                boost::asio::post(io_executor, [this, self]() {
                    send_json_response(http::status::internal_server_error, json{{"error", "Ошибка сервера при регистрации"}});
                });
            }
        });
        return;
    }
            else {
        send_json_response(http::status::not_found, json{{"error", "Not Found"}});
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