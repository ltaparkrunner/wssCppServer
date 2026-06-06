// handlers.cpp
#include "wssSession.hpp" // Важно подключить заголовочный файл класса
#include <iostream>
#include <regex>
#include <boost/core/ignore_unused.hpp>
#include <bsoncxx/builder/stream/document.hpp>

// 1. Метод handle_list_request
void WssSession::handle_list_request(const FilesFoldersListRequest& req) {
    boost::ignore_unused(req);
    // Ваша логика здесь
}

// 2. Метод handle_delete_file
void WssSession::handle_delete_file(const DeleteFileRequest& req) {
    boost::ignore_unused(req);
    // Ваша логика здесь
}

// 3. Метод handle_files_ids_request
void WssSession::handle_files_ids_request(const FilesIds& req) {
    boost::ignore_unused(req);
    // Ваша логика здесь
}

void WssSession::handle_path_inf_request(const PathInfoRequest& req) {
    // Захватываем shared_ptr на текущую сессию, чтобы предотвратить её деструкцию
    auto self = shared_from_this();

    // Получаем исполнителя (thread pool / io_context) для фонового выполнения
    auto thread_pool_executor = ws_.get_executor(); 

    // Переносим тяжелую блокирующую логику (БД и AWS S3) в пул потоков Asio
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            std::string input_path = req.netpath();
            std::string users_prefix = "USERS"; 
            std::string prefix = users_prefix + "/" + user_id_;
            std::string formatted_path = input_path;

            // 1. Обеспечиваем наличие префикса в пути
            if (formatted_path.rfind(prefix, 0) != 0) { 
                if (formatted_path.rfind(users_prefix, 0) == 0) {
                    formatted_path = prefix + formatted_path.substr(users_prefix.length());
                } else {
                    formatted_path = prefix + "/" + formatted_path;
                }
            }

            bool is_explicit_folder = !formatted_path.empty() && formatted_path.back() == '/';
            
            // Лямбда для экранирования спецсимволов в regex (аналог escapeRegex)
            auto escape_regex = [](const std::string& str) {
                std::regex special_chars(R"([-\\^$*+?.()|[\]{}])");
                return std::regex_replace(str, special_chars, R"(\$&)");
            };

            // Извлекаем базу данных из вашей обертки db_
            auto mongo_db = db_.get_db(); 
            auto collection = mongo_db["ImageRecord"];

            // ИСПОЛЬЗОВАНИЕ ВАШЕГО СТРУКТУРЫ CONFIG напрямую через публичные поля:
            std::string s3_endpoint = cfg_.s3_endpoint; 
            std::string bucket_name = cfg_.s3_bucket;
            int s3_expires = 360; // Значение по умолчанию (в Config этого поля нет)

            // --- КЕЙС 1: Путь заканчивается на "/" ---
            if (is_explicit_folder) {
                using bsoncxx::builder::stream::document;
                using bsoncxx::builder::stream::finalize;

                std::string pattern = "^" + escape_regex(formatted_path);
                auto query = document{} << "folder" << bsoncxx::types::b_regex{pattern} << finalize;
                
                mongocxx::options::find opts{};
                opts.projection(document{} << "_id" << 1 << finalize);

                auto doc = collection.find_one(query.view(), opts);

                if (doc) {
                    // Возвращаем вызовы в поток сокета (I/O thread) для безопасной отправки
                    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
                        FilesFoldersListRequest list_req;
                        list_req.set_foldername(input_path);
                        this->handle_list_request(list_req); 

                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        auto* path_resp = response.mutable_pathinfresponse();
                        path_resp->set_netpath(s3_endpoint + "/" + bucket_name + "/" + input_path);
                        path_resp->set_netstorepath("");
                        path_resp->set_result("folder");

                        this->send_envelope(response);
                    });
                    return;
                } else {
                    send_not_exist_response_async(input_path, s3_endpoint, bucket_name);
                    return;
                }
            }

            // --- КЕЙС 2: Проверка папки без явного слэша ---
            std::string folder_with_slash = formatted_path + "/";
            {
                using bsoncxx::builder::stream::document;
                using bsoncxx::builder::stream::finalize;

                std::string pattern = "^" + escape_regex(folder_with_slash);
                auto folder_query = document{} << "folder" << bsoncxx::types::b_regex{pattern} << finalize;
                
                mongocxx::options::find opts{};
                opts.projection(document{} << "_id" << 1 << finalize);

                auto folder_doc = collection.find_one(folder_query.view(), opts);

                if (folder_doc) {
                    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
                        FilesFoldersListRequest list_req;
                        list_req.set_foldername(input_path);
                        this->handle_list_request(list_req); 

                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        auto* path_resp = response.mutable_pathinfresponse();
                        path_resp->set_netpath(s3_endpoint + "/" + bucket_name + "/" + input_path + "/");
                        path_resp->set_netstorepath("");
                        path_resp->set_result("folder");

                        this->send_envelope(response);
                    });
                    return;
                }
            }

            // --- КЕЙС 3: Поиск файла по s3Key или комбинации (folder + originalName) ---
            {
                using bsoncxx::builder::stream::document;
                using bsoncxx::builder::stream::open_array;
                using bsoncxx::builder::stream::close_array;
                using bsoncxx::builder::stream::open_document;
                using bsoncxx::builder::stream::close_document;
                using bsoncxx::builder::stream::finalize;

                auto file_query = document{} 
                    << "$or" << open_array 
                        << open_document << "s3Key" << formatted_path << close_document
                        << open_document 
                            << "$expr" << open_document 
                                << "$eq" << open_array 
                                    << open_document << "$concat" << open_array << "$folder" << "$originalName" << close_array << close_document
                                    << formatted_path 
                                << close_array 
                            << close_document 
                        << close_document
                    << close_array 
                    << finalize;

                mongocxx::options::find opts{};
                opts.projection(document{} 
                    << "_id" << 1 
                    << "s3Key" << 1 
                    << "originalName" << 1 
                    << "size" << 1 
                    << finalize);

                auto file_doc = collection.find_one(file_query.view(), opts);

                if (file_doc) {
                    auto view = file_doc->view();
                    // std::string s3_key = view["s3Key"].get_string().value.to_string();
                    // std::string original_name = view["originalName"].get_string().value.to_string();
                    std::string s3_key { view["s3Key"].get_string().value };
                    std::string original_name { view["originalName"].get_string().value };
                    int64_t file_size = view["size"] ? view["size"].get_int64().value : 0;
                    std::string mongo_id = view["_id"].get_oid().value.to_string();

                    // Генерация AWS S3 Signed URL
                    Aws::Http::URI uri = s3_client_.GeneratePresignedUrl(
                        bucket_name, 
                        s3_key, 
                        Aws::Http::HttpMethod::HTTP_GET, 
                        s3_expires
                    );
                    std::string signed_url = uri.GetURIString();

                    // Безопасно передаем данные в поток I/O для отправки клиенту
                    boost::asio::post(ws_.get_executor(), [this, self, original_name, mongo_id, signed_url, file_size]() {
                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        auto* files_resp = response.mutable_filesidsresponse();
                        
                        auto* file_info = files_resp->add_files(); 
                        file_info->set_filename(original_name);
                        file_info->set_mongoid(mongo_id);
                        file_info->set_url(signed_url);
                        file_info->set_size(file_size);

                        this->send_envelope(response);
                    });
                    return;
                }
            }

            // --- КЕЙС 4: Ничего не найдено ---
            send_not_exist_response_async(input_path, s3_endpoint, bucket_name);

        } catch (const std::exception& e) {
            std::cerr << "Exception in handle_path_inf_request thread: " << e.what() << std::endl;
        }
    });
}

// Хелпер тоже выносим в handlers.cpp
void WssSession::send_not_exist_response_async(const std::string& input_path, const std::string& s3_endpoint, const std::string& bucket_name) {
    auto self = shared_from_this();
    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
        ServerEnvelope response;
        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
        auto* path_resp = response.mutable_pathinfresponse();
        path_resp->set_netpath(s3_endpoint + "/" + bucket_name + "/" + input_path + "/");
        path_resp->set_netstorepath("");
        path_resp->set_result("not exist");

        this->send_envelope(response);
    });
}
