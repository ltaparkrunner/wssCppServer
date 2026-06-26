// handlers.cpp
#include "wssSession.hpp" // Важно подключить заголовочный файл класса
#include "auxil.hpp"
#include <iostream>
#include <regex>
#include <boost/core/ignore_unused.hpp>
#include <bsoncxx/builder/stream/document.hpp>

void WssSession::handle_user_bucket(const BucketsRequest& req) {
    std::cout << "handle_user_bucket" << std::endl;
    auto self = shared_from_this();
    auto thread_pool_executor = ws_.get_executor();

    // Уходим в пул потоков, так как синхронный запрос к S3 заблокирует сокет
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            std::string bucket_name = cfg_.s3_bucket;
            std::string s3_endpoint = cfg_.s3_endpoint;
            // AWS S3: Проверяем существование бакета через HeadBucket
            Aws::S3::Model::HeadBucketRequest head_bucket_req;
            head_bucket_req.SetBucket(bucket_name);

            auto head_bucket_outcome = s3_client_.HeadBucket(head_bucket_req);
            std::cout << "boost::asio::post(thread_pool_executor, [this, self, req]() bucket_name: " << bucket_name 
            << "head_bucket_outcome.IsSuccess(): " << head_bucket_outcome.IsSuccess() << std::endl;
            if (head_bucket_outcome.IsSuccess()) {
                // Бакет существует: формируем успешный ответ аналогично Node.js
                std::cout << "bucketInfo: " << bucket_name << std::endl;

                // Получаем endpoint из конфигурации s3_client (в AWS SDK C++ это клиентская конфигурация)
                // Если у вас кастомный эндпоинт (например, MinIO), подставьте его из cfg_ или s3_client_
                //std::string s3_url = "https://" + bucket_name + ":/"; 
                std::string s3_url = s3_endpoint  + bucket_name + "/"; 
                // Если используется кастомный эндпоинт: "http://127.0.0" + bucket_name + "/"

                // Возвращаемся в I/O поток для отправки сообщения
                boost::asio::post(ws_.get_executor(), [this, self, bucket_name, s3_url]() {
                    ServerEnvelope response;
                    response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                    
                    // Заполняем структуру buckets -> bucketInf на основе вашей Protobuf схемы
                    auto* buckets_msg = response.mutable_buckets();
                    auto* bucket_info = buckets_msg->add_bucketinf(); // Предполагается, что это repeated поле
                    bucket_info->set_bucketname(bucket_name);
                    bucket_info->set_url(s3_url);
                    std::cout << "bucket_name: " << bucket_name << "  s3_url: " << s3_url << std::endl;
                    this->send_envelope(response);
                });

            } else {
                // Бакет не найден или произошла другая ошибка (например, 404 или Access Denied)
                auto aws_error = head_bucket_outcome.GetError();
                auto status_code = aws_error.GetResponseCode();

                if (status_code == Aws::Http::HttpResponseCode::NOT_FOUND) {
                    // Ошибка 404: Бакет не найден
                    boost::asio::post(ws_.get_executor(), [this, self, aws_error]() {
                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        
                        auto* server_resp = response.mutable_serverresp();
                        server_resp->set_content("Bucket images not found");
                        // Передаем ошибку в поле status (в Protobuf это строка или структура)
                        server_resp->set_status(aws_error.GetMessage()); 

                        std::cout << "Response for non-existing path (404): " << aws_error.GetMessage() << std::endl;
                        this->send_envelope(response);
                    });
                } else {
                    // Любая другая ошибка (например, 403 Access Denied)
                    boost::asio::post(ws_.get_executor(), [this, self, aws_error]() {
                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        
                        auto* server_resp = response.mutable_serverresp();
                        server_resp->set_content("Access denied or other error: " + aws_error.GetMessage());
                        server_resp->set_status(aws_error.GetMessage());

                        std::cout << "Response for non-existing path (Error): " << aws_error.GetMessage() << std::endl;
                        this->send_envelope(response);
                    });
                }
            }

        } catch (const std::exception& e) {
            std::cerr << "Error in handle_user_bucket: " << e.what() << std::endl;
            
            // В случае непредвиденного исключения отправляем общую ошибку
            boost::asio::post(ws_.get_executor(), [this, self, error_msg = std::string(e.what())]() {
                ServerEnvelope response;
                response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                auto* server_resp = response.mutable_serverresp();
                server_resp->set_content("Access denied or other error: " + error_msg);
                server_resp->set_status("EXCEPTION");
                this->send_envelope(response);
            });
        }
    });
}

void WssSession::handle_add_file(const AddFileRequest& req) {
    auto self = shared_from_this();
    auto thread_pool_executor = ws_.get_executor();

    // Уходим в пул потоков, так как операции с S3 и Mongo заблокируют сокет
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            std::string file_name = req.filename();
            std::string folder_name = req.folder();
            std::string info = req.info();
            
            // В Protobuf поле типа 'bytes' в C++ мапится в std::string, содержащую бинарные данные
            std::string img_data = req.data(); 
            std::size_t img_data_length = img_data.length();

            std::cout << "Buffer size: " << img_data_length << std::endl;

            auto mongo_db = db_.get_db();
            auto users_collection = mongo_db["users"];
            auto records_collection = mongo_db["ImageRecord"];

            // 1. Ищем логин пользователя по его user_id_ (ObjectId)
            std::string usr_login = "Unknown";
            try {
                bsoncxx::oid user_oid(user_id_);
                auto user_doc = users_collection.find_one(
                    bsoncxx::builder::stream::document{} << "_id" << user_oid << bsoncxx::builder::stream::finalize
                );
                if (user_doc) {
                    usr_login = std::string{user_doc->view()["login"].get_string().value};
                }
            } catch (const std::exception& e) {
                std::cerr << "Failed to find user for login logging: " << e.what() << std::endl;
            }

            // 2. Формируем targetFolder аналогично Node.js
            std::string users_prefix = "USERS";
            std::string user_base_path = users_prefix + "/" + user_id_;
            std::string target_folder;

            if (folder_name.rfind(user_base_path, 0) == 0) {
                target_folder = (folder_name.back() == '/') ? folder_name : (folder_name + "/");
            } else {
                if (folder_name.empty()) {
                    target_folder = user_base_path + "/";
                } else {
                    target_folder = user_base_path + "/" + folder_name + "/";
                }
            }

            // 3. ПРОВЕРКА НА УНИКАЛЬНОСТЬ ИМЕНИ ФАЙЛА В MONGODB
            // Разделяем имя и расширение с помощью вашей структуры/логики из prepare_filename
            FilenameInfo fn_info = prepare_filename(file_name);
            
            // Сохраняем базовое имя и расширение для генерации новых вариантов
            std::string base_name = fn_info.unique_name; // Предполагается, что это поле есть в FilenameInfo
            std::string ext = fn_info.ext;             // Например, ".jpg" или "jpg"
            
            // Нормализуем точку в расширении
            std::string ext_with_dot = ext;
            if (!ext_with_dot.empty() && ext_with_dot[0] != '.') {
                ext_with_dot = "." + ext_with_dot;
            }

            std::string final_original_name = file_name;
            int counter = 1;

            using bsoncxx::builder::stream::document;
            using bsoncxx::builder::stream::finalize;

            // Цикл работает до тех пор, пока в данной папке существует файл с таким originalName
            while (true) {
                auto duplicate_query = document{} 
                    << "folder" << target_folder 
                    << "originalName" << final_original_name 
                    << finalize;
                
                auto exists = records_collection.find_one(duplicate_query.view());
                if (!exists) {
                    // Файла с таким именем в папке нет, имя свободно!
                    break;
                }

                // Если файл найден, пересобираем имя: "имя(1).ext", "имя(2).ext"
                final_original_name = base_name + "(" + std::to_string(counter) + ")" + ext_with_dot;
                counter++;
            }

            // Корректируем fn_info под новое уникальное имя, если оно изменилось
            if (counter > 1) {
                std::cout << "Filename collision detected! Renamed to: " << final_original_name << std::endl;
                // Перегенерируем внутреннее уникальное имя (для UUID s3Key, если ваша prepare_filename это делает)
                // Или просто обновляем имена, в зависимости от логики вашего метода:
                fn_info = prepare_filename(final_original_name);
            }

            std::string s3_key = target_folder + fn_info.unique_name;
            std::string bucket_name = cfg_.s3_bucket;

            std::cout << " fileName= " << final_original_name 
                      << " usrLogin = " << usr_login
                      << " folder = " << folder_name 
                      << " info = " << info << std::endl;

            std::cout << "uniqueName: " << fn_info.unique_name 
                      << " s3Key= " << s3_key 
                      << " targetFolder: " << target_folder 
                      << " ext: " << fn_info.ext << std::endl;

            // 4. AWS S3: Загружаем файл через PutObjectRequest
            Aws::S3::Model::PutObjectRequest put_obj_req;
            put_obj_req.SetBucket(bucket_name);
            put_obj_req.SetKey(s3_key);

            auto request_stream = std::make_shared<std::stringstream>(img_data);
            put_obj_req.SetBody(request_stream);

            auto put_obj_outcome = s3_client_.PutObject(put_obj_req);
            if (!put_obj_outcome.IsSuccess()) {
                throw std::runtime_error("Failed to upload file to S3: " + put_obj_outcome.GetError().GetMessage());
            }
            std::cout << " s3Client.send(command) successful " << s3_key << std::endl;

            // 5. MongoDB: Сохраняем метаданные в ImageRecord
            using bsoncxx::builder::stream::open_document;
            using bsoncxx::builder::stream::close_document;

            auto meta_doc = document{}
                << "name" << fn_info.unique_name
                << "originalName" << final_original_name // Сохраняем уже новое имя с (1), если был дубликат
                << "folder" << target_folder
                << "s3Key" << s3_key
                << "bucket" << bucket_name
                << "userLogin" << usr_login
                << "info" << open_document 
                    << "type" << fn_info.ext 
                << close_document
                << "size" << static_cast<int64_t>(img_data_length)
                << finalize;

            auto insert_res = records_collection.insert_one(meta_doc.view());
            if (insert_res) {
                std::string mongo_id = insert_res->inserted_id().get_oid().value.to_string();
                std::cout << "Saved image " << fn_info.unique_name 
                          << " for user " << usr_login 
                          << " with ID: " << mongo_id << std::endl;
            }

            // 6. Возвращаем отправку успешного ответа в основной I/O поток сокета
            boost::asio::post(ws_.get_executor(), [this, self]() {
                ServerEnvelope response;
                response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                
                auto* server_resp = response.mutable_serverresp(); 
                server_resp->set_content("upload_result");
                server_resp->set_status("success");

                this->send_envelope(response);
            });

        } catch (const std::exception& e) {
            std::cerr << "Error in handle_add_file: " << e.what() << std::endl;
        }
    });
}

void WssSession::handle_list_request(const FilesFoldersListRequest& req) {
    auto self = shared_from_this();
    auto thread_pool_executor = ws_.get_executor();

    // Переносим тяжелую логику (БД и S3) в пул потоков
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            // В зависимости от вашего .proto имя сеттера/геттера может быть foldername() или folder_name()
            std::string folder_name = req.foldername();
            std::cout << "function handleListRequest folderName: " << folder_name << std::endl;

            if (!folder_name.empty() && folder_name.back() == '/') {
                folder_name.pop_back();
            }

            std::string users_prefix = "USERS"; // Аналог переменной ${USERS}
            std::string user_base_path = users_prefix + "/" + user_id_;
            std::string target_folder;

            // Формируем targetFolder аналогично логике на Node.js
            if (folder_name.rfind(user_base_path, 0) == 0) {
                target_folder = (folder_name.back() == '/') ? folder_name : (folder_name + "/");
            } else {
                if (folder_name.empty()) {
                    target_folder = user_base_path + "/";
                } else {
                    // Используем вашу функцию очистки пути sanitizeToPath, если она определена, 
                    // либо подставляем строку напрямую
                    target_folder = user_base_path + "/" + folder_name + "/";
                }
            }

            std::string bucket_name = cfg_.s3_bucket;
            std::string s3_endpoint = cfg_.s3_endpoint;
            std::cout << "targetFolder: " << target_folder << "  BUCKET: " << bucket_name << std::endl;

            auto mongo_db = db_.get_db();
            auto collection = mongo_db["ImageRecord"];

            // --- 1. ПОЛУЧАЕМ РЕАЛЬНЫЕ ФАЙЛЫ В ПАПКЕ (ImageRecord.find) ---
            using bsoncxx::builder::stream::document;
            using bsoncxx::builder::stream::finalize;
            using bsoncxx::builder::stream::open_array;
            using bsoncxx::builder::stream::close_array;
            using bsoncxx::builder::stream::open_document;
            using bsoncxx::builder::stream::close_document;

            auto files_query = document{} << "bucket" << bucket_name 
                                          << "folder" << target_folder 
                                          << finalize;
            auto files_cursor = collection.find(files_query.view());

            //  Структуры для накопления ответов перед отправкой в поток сокета
            struct FilePayloadItem {
                std::string file_name;
                std::string mongo_id;
                std::string url;
                int64_t size;
            };
            std::vector<FilePayloadItem> files_payload;

//            int s3_expires = cfg_.get_s3_expires() > 0 ? cfg_.get_s3_expires() : 3600;
            int s3_expires = cfg_.s3_ref_expires > 0 ? cfg_.s3_ref_expires : 3600;

            for (auto&& doc : files_cursor) {
                //  auto view = doc.view();
                std::string s3_key{ doc["s3Key"].get_string().value };
                std::string original_name{ doc["originalName"].get_string().value };
                int64_t file_size = doc["size"] ? doc["size"].get_int64().value : 0;
                std::string mongo_id = doc["_id"].get_oid().value.to_string();

                // Генерация временной ссылки S3 (Presigned URL)
                Aws::Http::URI uri = s3_client_.GeneratePresignedUrl(
                    bucket_name, 
                    s3_key, 
                    Aws::Http::HttpMethod::HTTP_GET, 
                    s3_expires
                );

                files_payload.push_back({original_name, mongo_id, uri.GetURIString(), file_size});
            }
            std::cout << "Real files in folder count: " << files_payload.size() << std::endl;

            // --- 2. НАХОДИМ ВИРТУАЛЬНЫЕ ПОДПАПКИ (ImageRecord.aggregate) ---
            mongocxx::pipeline pipeline;
            
            // Стейдж 1: $match по регулярному выражению ^targetFolder[^/]+
            std::string regex_pattern = "^" + target_folder + "[^/]+";
            pipeline.match(document{} << "bucket" << bucket_name 
                                      << "folder" << bsoncxx::types::b_regex{regex_pattern} 
                                      << finalize);

            // Стейдж 2: $project для relativeFolder (обрезаем префикс текущей папки)
            pipeline.project(document{} 
                << "relativeFolder" << open_document 
                    << "$substr" << open_array << "$folder" << static_cast<int32_t>(target_folder.length()) << -1 << close_array 
                << close_document 
                << finalize);

            // Стейдж 3: $project для folderNm (берем первый сегмент до слэша)
            pipeline.project(document{} 
                << "folderNm" << open_document 
                    << "$arrayElemAt" << open_array 
                        << open_document << "$split" << open_array << "$relativeFolder" << "/" << close_array << close_document 
                        << 0 
                    << close_array 
                << close_document 
                << finalize);

            // Стейдж 4: $group по уникальным именам папок
            pipeline.group(document{} << "_id" << "$folderNm" << finalize);

            auto folders_cursor = collection.aggregate(pipeline);

            struct FolderPayloadItem {
                std::string folder_name;
                std::string url;
            };
            std::vector<FolderPayloadItem> folders_payload;

            // Нормализуем эндпоинт для конструирования путей
            std::string minio_path = s3_endpoint;
            if (!minio_path.empty() && minio_path.back() != '/') minio_path += "/";
            minio_path += bucket_name + "/";

            for (auto&& doc : folders_cursor) {
                // auto view = doc.view();
                if (!doc["_id"] || doc["_id"].type() == bsoncxx::type::k_null) continue;
                
                std::string folder_nm{ doc["_id"].get_string().value };
                std::string folder_url = folder_name.empty() 
                    ? (minio_path + folder_nm + "/") 
                    : (minio_path + folder_name + "/" + folder_nm + "/");

                folders_payload.push_back({folder_nm, folder_url});
            }
            std::cout << "Real folders in folder count: " << folders_payload.size() << std::endl;
            std::cout << "minio_path: " << minio_path << std::endl;
            // --- 3. СБОРКА И ОТПРАВКА ОТВЕТА В ПОТОКЕ СОКЕТА ---
            boost::asio::post(ws_.get_executor(), [this, self, files_payload, folders_payload, target_folder]() {
                ServerEnvelope response;
                response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                auto* list_resp = response.mutable_listresponse();

                // Заполняем файлы в Protobuf (repeated поле)
                for (const auto& file : files_payload) {
                    auto* f = list_resp->add_files();
                    f->set_filename(file.file_name);
                    f->set_mongoid(file.mongo_id);
                    std::cout << "file.url: " << file.url << std::endl;
                    f->set_url(file.url);
                    f->set_size(file.size);
                }

                // Заполняем папки в Protobuf (repeated поле)
                for (const auto& folder : folders_payload) {
                    auto* f = list_resp->add_folders();
                    f->set_foldername(folder.folder_name);
                    std::cout << "folder.url: " << folder.url << std::endl;
                    f->set_url(folder.url);
                } 
                list_resp->set_foldername(target_folder);
                std::cout << "Final response payload prepared. Sending... netPath:" /* << response.netpath() */ << std::endl;
                this->send_envelope(response);
            });

        } catch (const std::exception& e) {
            std::cerr << "Error in handle_list_request: " << e.what() << std::endl;
        }
    });
}

void WssSession::handle_delete_file(const DeleteFileRequest& req) {
    auto self = shared_from_this();
    auto thread_pool_executor = ws_.get_executor();

    // Переносим блокирующую логику работы с БД и S3 в пул потоков
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            // В зависимости от вашей .proto-схемы имя геттера может быть mongoid() или mongo_id()
            std::string mongo_id_str = req.mongoid(); 
            std::string file_name = req.filename(); // Передается для логирования (fname в JS)

            std::cout << "deleteFile: fname = " << file_name 
                      << " mongoId = " << mongo_id_str << std::endl;

            auto mongo_db = db_.get_db();
            auto collection = mongo_db["ImageRecord"];

            // 1. Поиск записи в MongoDB по её ObjectId
            bsoncxx::oid mongo_oid(mongo_id_str);
            using bsoncxx::builder::stream::document;
            using bsoncxx::builder::stream::finalize;

            auto query = document{} << "_id" << mongo_oid << finalize;
            auto record_opt = collection.find_one(query.view());

            if (!record_opt) {
                std::cerr << "Запись не найдена в базе данных" << std::endl;
                // При необходимости здесь можно отправить клиенту пакет со статусом "error"
                return;
            }

            // Извлекаем s3Key из найденного документа
            auto record_view = record_opt->view();
            // std::string s3_key{ record_view["s3Key"].get_string().value };
            // std::string s3_key = record_view["s3Key"].get_string().value.to_string();
            std::string s3_key = std::string(record_view["s3Key"].get_string().value);
            std::string bucket_name = cfg_.s3_bucket;

            // 2. AWS S3: Удаление физического объекта из бакета
            Aws::S3::Model::DeleteObjectRequest delete_obj_req;
            delete_obj_req.SetBucket(bucket_name);
            delete_obj_req.SetKey(s3_key);

            auto delete_obj_outcome = s3_client_.DeleteObject(delete_obj_req);
            if (!delete_obj_outcome.IsSuccess()) {
                throw std::runtime_error("Не удалось удалить файл из S3: " + delete_obj_outcome.GetError().GetMessage());
            }
            std::cout << "Файл " << s3_key << " удален из MinIO" << std::endl;

            // 3. MongoDB: Удаление метаданных из коллекции
            collection.delete_one(query.view());
            std::cout << "Запись " << mongo_id_str << " удалена из MongoDB" << std::endl;

            // 4. Возвращаем отправку успешного ответа в основной I/O поток сокета
            boost::asio::post(ws_.get_executor(), [this, self, file_name, bucket_name]() {
                ServerEnvelope response;
                response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                
                auto* server_resp = response.mutable_serverresp();
                server_resp->set_content("delete_file_result");
                server_resp->set_status("success");

                this->send_envelope(response);
                std::cout << "Delete document off " << file_name << " from " << bucket_name << std::endl;
            });

        } catch (const std::exception& e) {
            std::cerr << "Error in handle_delete_file: " << e.what() << std::endl;
            // Код обработки ошибок (опционально: отправка status: "error" клиенту)
        }
    });
}

void WssSession::handle_files_ids_request(const FilesIds& req) {
    auto self = shared_from_this();
    auto thread_pool_executor = ws_.get_executor();

    // Отправляем тяжелую логику работы с БД и AWS S3 в пул потоков
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            // Структура для временного хранения элементов ответа перед отправкой в поток сокета
            struct FileInfoItem {
                std::string file_name;
                std::string mongo_id;
                std::string url;
                int64_t size;
            };
            std::vector<FileInfoItem> files_payload;

            auto mongo_db = db_.get_db();
            auto collection = mongo_db["ImageRecord"];
            
            std::string bucket_name = cfg_.s3_bucket;
            int s3_expires = 360; // Значение по умолчанию (аналог || 360 из JS)

            // Проходим по всем MongoDB ID, переданным в Protobuf (поле repeated)
            // ПРИМЕЧАНИЕ: Имя геттера (например, mongoids()) зависит от вашего .proto файла
            for (int i = 0; i < req.mongoids_size(); ++i) {
                std::string id_str = req.mongoids(i);
                std::cout << "Processing ID: " << id_str << std::endl;

                try {
                    bsoncxx::oid mongo_oid(id_str);
                    using bsoncxx::builder::stream::document;
                    using bsoncxx::builder::stream::finalize;

                    auto query = document{} << "_id" << mongo_oid << finalize;
                    auto record_opt = collection.find_one(query.view());

                    if (record_opt) {
                        auto record_view = record_opt->view();
                        
                        // ИСПРАВЛЕНО: Безопасное конструирование строк из string_view через явный конструктор std::string
                        std::string s3_key = std::string(record_view["s3Key"].get_string().value);
                        std::string original_name = std::string(record_view["originalName"].get_string().value);
                        int64_t file_size = record_view["size"] ? record_view["size"].get_int64().value : 0;
                        std::string mongo_id = record_view["_id"].get_oid().value.to_string();

                        // Генерация временной ссылки S3 (Presigned URL)
                        Aws::Http::URI uri = s3_client_.GeneratePresignedUrl(
                            bucket_name, 
                            s3_key, 
                            Aws::Http::HttpMethod::HTTP_GET, 
                            s3_expires
                        );

                        files_payload.push_back({original_name, mongo_id, uri.GetURIString(), file_size});
                    } else {
                        std::cout << "Record with ID " << id_str << " not found in MongoDB" << std::endl;
                        files_payload.push_back({"", id_str, "", 0});
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Invalid ObjectId format or DB error for ID " << id_str << ": " << e.what() << std::endl;
                    files_payload.push_back({"", id_str, "", 0});
                }
            }

            // Возвращаем сборку и отправку Protobuf-пакета обратно в I/O поток сокета (strand)
            boost::asio::post(ws_.get_executor(), [this, self, files_payload]() {
                ServerEnvelope response;
                response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                
                // В зависимости от вашей схемы имя может быть mutable_filesidsresponse()
                auto* files_resp = response.mutable_filesidsresponse();

                // Заполняем repeated поле файлов в Protobuf-сообщении
                for (const auto& item : files_payload) {
                    auto* file_info = files_resp->add_files(); // Метод add_...() для repeated полей
                    file_info->set_filename(item.file_name);
                    file_info->set_mongoid(item.mongo_id);
                    file_info->set_url(item.url);
                    file_info->set_size(item.size);
                }

                this->send_envelope(response);
            });

        } catch (const std::exception& e) {
            std::cerr << "Error in handle_files_ids_request: " << e.what() << std::endl;
        }
    });
}

void WssSession::handle_file_path(const FilePathRequest& req) {
    // Захватываем shared_ptr на текущую сессию, чтобы предотвратить её деструкцию
    auto self = shared_from_this();

    // Получаем исполнителя (thread pool / io_context) для фонового выполнения
    auto thread_pool_executor = ws_.get_executor(); 

    // Переносим тяжелую блокирующую логику (БД и AWS S3) в пул потоков Asio
    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            std::string input_path = req.netpath();
            std::cout << "Processing FilePathRequest for: " << input_path << std::endl;

            // Структура для временного хранения элементов ответа (совпадает по логике с вашей)
            struct FileInfoItem {
                std::string file_name;
                std::string mongo_id;
                std::string url;
                int64_t size;
            };
            FileInfoItem file_payload;
            bool is_file_found = false;
            std::string current_folder_path = "";

            auto mongo_db = db_.get_db();
            auto collection = mongo_db["ImageRecord"];
            
            std::string bucket_name = cfg_.s3_bucket;
            int s3_expires = 360; // Значение по умолчанию из вашего конфига

            using bsoncxx::builder::stream::document;
            using bsoncxx::builder::stream::finalize;

            // 1. Ищем конкретный файл по его полному пути в системе
            // ПРИМЕЧАНИЕ: Имя поля в БД ("netpath") должно соответствовать вашей схеме ImageRecord
            std::string users_prefix = "USERS";
            std::string user_base_path = users_prefix + "/" + user_id_;
            auto query = document{} << "s3Key" << user_base_path + "/" + input_path << finalize;
            std::cout << "query: " << bsoncxx::to_json(query.view()) << std::endl;
            auto record_opt = collection.find_one(query.view());

            if (record_opt) {
                // --- СЦЕНАРИЙ 1: Файл найден ---

                auto record_view = record_opt->view();
                is_file_found = true;
                std::cout << "The file is found: " << record_view["s3Key"].get_string().value << std::endl;
                std::string s3_key = std::string(record_view["s3Key"].get_string().value);
                std::string original_name = std::string(record_view["originalName"].get_string().value);
                int64_t file_size = record_view["size"] ? record_view["size"].get_int64().value : 0;
                std::string mongo_id = record_view["_id"].get_oid().value.to_string();

                // Генерация временной ссылки S3 (Presigned URL) через ваше AWS SDK
                Aws::Http::URI uri = s3_client_.GeneratePresignedUrl(
                    bucket_name, 
                    s3_key, 
                    Aws::Http::HttpMethod::HTTP_GET, 
                    s3_expires
                );

                file_payload = {original_name, mongo_id, uri.GetURIString(), file_size};
                boost::asio::post(ws_.get_executor(), [this, self, file_payload]() {
                    ServerEnvelope response;
                    response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
    
                    auto* files_resp = response.mutable_filepathresponse();
                    files_resp->set_filename(file_payload.file_name);
                    files_resp->set_mongoid(file_payload.mongo_id);
                    files_resp->set_url(file_payload.url);
                    files_resp->set_size(file_payload.size);
    
                    this->send_envelope(response);
                });
            } 
            else {
                // --- СЦЕНАРИЙ 2: Файл НЕ найден -> Вызываем handle_user_bucket ---
                std::string s3_endpoint = cfg_.s3_endpoint; 
                boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
                    FilesFoldersListRequest list_req;
                    list_req.set_foldername("");
                    std::cout << ":handle_path_inf_request input_path 2 : " << input_path << std::endl;
                    this->handle_list_request(list_req); 

                    ServerEnvelope response;
                    response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                    auto* path_resp = response.mutable_pathinfresponse();
                    path_resp->set_netpath(s3_endpoint + bucket_name + "/" + input_path + "/");
                    path_resp->set_netstorepath("");
                    path_resp->set_result("folder");

                    this->send_envelope(response);
                });
            }
        }
        catch (const std::exception& e) {
            std::clog << "Exception in handle_file_path thread: " << e.what() << std::endl;
            std::cerr << "Exception in handle_file_path thread: " << e.what() << std::endl;
        }
    });
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

            std::cout << "input_path: " << input_path << "  prefix: " << prefix << "  formatted_path: " 
            << formatted_path << std::endl;

            // 1. Обеспечиваем наличие префикса в пути
            if (formatted_path.rfind(prefix, 0) != 0) { 
                if (formatted_path.rfind(users_prefix, 0) == 0) {
                    formatted_path = prefix + formatted_path.substr(users_prefix.length());
                } else {
                    formatted_path = prefix + "/" + formatted_path;
                }
            }

            std::cout << "input_path: " << input_path << "  prefix: " << prefix << "  formatted_path: " 
            << formatted_path << std::endl;

            bool is_explicit_folder = !formatted_path.empty() && formatted_path.back() == '/';

            // Извлекаем базу данных из вашей обертки db_
            auto mongo_db = db_.get_db(); 
            auto collection = mongo_db["ImageRecord"];

            // ИСПОЛЬЗОВАНИЕ ВАШЕГО СТРУКТУРЫ CONFIG напрямую через публичные поля:
            std::string s3_endpoint = cfg_.s3_endpoint; 
            std::string bucket_name = cfg_.s3_bucket;
            int s3_expires = 360; // Значение по умолчанию (в Config этого поля нет)

            std::cout << "is3_endpoint : " << s3_endpoint << "  bucket_name: " << bucket_name << std::endl;
            // --- КЕЙС 1: Путь заканчивается на "/" ---
            if (is_explicit_folder) {
                std::cout << " is_explicit_folder " << std::endl;
                using bsoncxx::builder::stream::document;
                using bsoncxx::builder::stream::finalize;

                std::string pattern = "^" + sanitizeToPath(formatted_path);
                auto query = document{} << "folder" << bsoncxx::types::b_regex{pattern} << finalize;

                std::cout << "query: " << bsoncxx::to_json(query.view()) << std::endl;
                
                mongocxx::options::find opts{};
                opts.projection(document{} << "_id" << 1 << finalize);

                auto doc = collection.find_one(query.view(), opts);

                if (doc) {
                    std::cout << " if (doc) " << std::endl;
                    // Возвращаем вызовы в поток сокета (I/O thread) для безопасной отправки
                    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
                        FilesFoldersListRequest list_req;
                        list_req.set_foldername(input_path);
                        std::cout << ":handle_path_inf_request input_path 1 : " << input_path << std::endl;
                        this->handle_list_request(list_req); 

                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        auto* path_resp = response.mutable_pathinfresponse();
                        path_resp->set_netpath(s3_endpoint + bucket_name + "/" + input_path);
                        std::cout << "mutable_pathinfresponse set_netpath: " << s3_endpoint + bucket_name + "/" + input_path << std::endl;
                        path_resp->set_netstorepath("");
                        path_resp->set_result("folder");

                        this->send_envelope(response);
                    });
                    return;
                } else {
                    std::cout << " if (doc) else " << std::endl;
                    send_not_exist_response_async(input_path, s3_endpoint, bucket_name);
                    return;
                }
            }

            // --- КЕЙС 2: Проверка папки без явного слэша ---
            std::string folder_with_slash = formatted_path + "/";
            {
                std::cout << "КЕЙС 2: Проверка папки без явного слэша is_explicit_folder " << std::endl;
                using bsoncxx::builder::stream::document;
                using bsoncxx::builder::stream::finalize;

                std::string pattern = "^" + sanitizeToPath(folder_with_slash);
                auto folder_query = document{} << "folder" << bsoncxx::types::b_regex{pattern} << finalize;
                
                mongocxx::options::find opts{};
                opts.projection(document{} << "_id" << 1 << finalize);

                auto folder_doc = collection.find_one(folder_query.view(), opts);

                if (folder_doc) {
                    std::cout << "folder_doc " << std::endl;
                    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
                        FilesFoldersListRequest list_req;
                        list_req.set_foldername(input_path);
                        std::cout << ":handle_path_inf_request input_path 2 : " << input_path << std::endl;
                        this->handle_list_request(list_req); 

                        ServerEnvelope response;
                        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                        auto* path_resp = response.mutable_pathinfresponse();
                        path_resp->set_netpath(s3_endpoint + bucket_name + "/" + input_path + "/");
                        path_resp->set_netstorepath("");
                        path_resp->set_result("folder");

                        this->send_envelope(response);
                    });
                    return;
                }
            }

            // --- КЕЙС 3: Поиск файла по s3Key или комбинации (folder + originalName) ---
            {
                std::cout << "КЕЙС 3: Поиск файла по s3Key или комбинации (folder + originalName) " << std::endl;
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
                    std::cout << "file_doc " << std::endl;
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
                        std::cout << ":handle_path_inf_request signed_url 3 : " << signed_url << std::endl;
                        this->send_envelope(response);
                    });
                    return;
                }
            }

            // --- КЕЙС 4: Ничего не найдено ---
            std::cout << "send_not_exist_response_async " << std::endl;
            //send_not_exist_response_async(input_path, s3_endpoint, bucket_name);
            send_bucket_content_response_async(self, input_path, s3_endpoint, bucket_name);
        } catch (const std::exception& e) {
            std::cout << "Exception in handle_path_inf_request thread: " << e.what() << std::endl;
            std::cerr << "Exception in handle_path_inf_request thread: " << e.what() << std::endl;
        }
    });
}

void WssSession::handle_rewrite_file(const RewriteFileRequest& req) {
    auto self = shared_from_this();
    auto thread_pool_executor = ws_.get_executor();

    boost::asio::post(thread_pool_executor, [this, self, req]() {
        try {
            std::string target_folder_name = req.folder(); // Целевая папка из запроса
            
            auto mongo_db = db_.get_db();
            auto records_collection = mongo_db["ImageRecord"];

            bsoncxx::oid file_oid(req.fileid());
            using bsoncxx::builder::stream::document;
            using bsoncxx::builder::stream::finalize;
            using bsoncxx::builder::stream::open_document;
            using bsoncxx::builder::stream::close_document;

            // 1. Ищем исходный файл по его ID
            auto query = document{} << "_id" << file_oid << finalize;
            auto record_opt = records_collection.find_one(query.view());

            if (!record_opt) {
                send_error_to_socket("File record not found", req.fileid());
                return;
            }

            auto record_view = record_opt->view();
            std::string source_s3_key = std::string(record_view["s3Key"].get_string().value);
            std::string original_name = std::string(record_view["originalName"].get_string().value);
            std::string bucket_name = cfg_.s3_bucket;

            // 2. Формируем target_folder по правилам из handle_add_file
            std::string users_prefix = "USERS";
            std::string user_base_path = users_prefix + "/" + user_id_;
            std::string target_folder;

            if (target_folder_name.rfind(user_base_path, 0) == 0) {
                target_folder = (target_folder_name.back() == '/') ? target_folder_name : (target_folder_name + "/");
            } else {
                if (target_folder_name.empty()) {
                    target_folder = user_base_path + "/";
                } else {
                    target_folder = user_base_path + "/" + target_folder_name + "/";
                }
            }

            // 3. Защита от коллизий (проверяем существование файла в целевой папке)
            size_t dot_idx = original_name.find_last_of(".");
            std::string base_name = (dot_idx == std::string::npos) ? original_name : original_name.substr(0, dot_idx);
            std::string ext = (dot_idx == std::string::npos) ? "" : original_name.substr(dot_idx);

            std::string current_name = original_name;
            std::string target_s3_key = target_folder + current_name;
            int counter = 1;

            while (true) {
                Aws::S3::Model::HeadObjectRequest head_req;
                head_req.SetBucket(bucket_name);
                head_req.SetKey(target_s3_key);

                auto head_outcome = s3_client_.HeadObject(head_req);
                if (!head_outcome.IsSuccess()) {
                    if (head_outcome.GetError().GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
                        break; // Имя свободно
                    }
                    send_error_to_socket("Storage error: " + head_outcome.GetError().GetMessage(), req.fileid());
                    return;
                }
                // Файл с таким именем уже есть, инкрементируем
                current_name = base_name + "(" + std::to_string(counter) + ")" + ext;
                target_s3_key = target_folder + current_name;
                counter++;
            }

            // 4. Server-Side Copy внутри MinIO
            Aws::S3::Model::CopyObjectRequest copy_req;
            copy_req.SetBucket(bucket_name);
            copy_req.SetKey(target_s3_key);
            copy_req.SetCopySource(bucket_name + "/" + source_s3_key);

            auto copy_outcome = s3_client_.CopyObject(copy_req);
            if (!copy_outcome.IsSuccess()) {
                send_error_to_socket("S3 Copy failed: " + copy_outcome.GetError().GetMessage(), req.fileid());
                return;
            }

            // [Вариант Б] 5. Вместо удаления создаем НОВЫЙ документ в MongoDB
            auto usr_login = record_view["userLogin"] ? std::string(record_view["userLogin"].get_string().value) : "Unknown";
            auto file_size = record_view["size"] ? record_view["size"].get_int64().value : 0;
            auto info_str  = record_view["info"] && record_view["info"]["type"] ? std::string(record_view["info"]["type"].get_string().value) : ext;

            auto meta_doc = document{}
                << "name" << current_name
                << "originalName" << original_name // Сохраняем самое первое исходное имя файла
                << "folder" << target_folder
                << "s3Key" << target_s3_key
                << "bucket" << bucket_name
                << "userLogin" << usr_login
                << "info" << open_document 
                    << "type" << info_str 
                << close_document
                << "size" << static_cast<int64_t>(file_size)
                << finalize;

            auto insert_res = records_collection.insert_one(meta_doc.view());
            if (insert_res) {
                std::string new_mongo_id = insert_res->inserted_id().get_oid().value.to_string();
                std::cout << "Copied file record successfully. New ID: " << new_mongo_id << std::endl;
            }

            // 7. Ответ в I/O поток сокета
            boost::asio::post(ws_.get_executor(), [this, self]() {
                ServerEnvelope response;
                response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
                auto* server_resp = response.mutable_serverresp();
                server_resp->set_content("rewrite_result");
                server_resp->set_status("success");
                this->send_envelope(response);
            });

        } catch (const std::exception& e) {
            std::cerr << "Error in handle_rewrite_file: " << e.what() << std::endl;
            send_error_to_socket(std::string("Exception: ") + e.what(), req.fileid());
        }
    });
}

void WssSession::send_bucket_content_response_async(const std::shared_ptr<WssSession> self, const std::string& input_path, const std::string& s3_endpoint, const std::string& bucket_name) {
//    std::string s3_endpoint = cfg_.s3_endpoint; 
    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
        FilesFoldersListRequest list_req;
        list_req.set_foldername("");
        std::cout << ":handle_path_inf_request input_path 2 : " << input_path << std::endl;
        this->handle_list_request(list_req); 

        ServerEnvelope response;
        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
        auto* path_resp = response.mutable_pathinfresponse();
        path_resp->set_netpath(s3_endpoint + bucket_name);
        path_resp->set_netstorepath("");
        path_resp->set_result("folder");

        this->send_envelope(response);
    });
}


// Хелпер тоже выносим в handlers.cpp
void WssSession::send_not_exist_response_async(const std::string& input_path, const std::string& s3_endpoint, const std::string& bucket_name) {
    auto self = shared_from_this();
    boost::asio::post(ws_.get_executor(), [this, self, input_path, s3_endpoint, bucket_name]() {
        ServerEnvelope response;
        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
        auto* path_resp = response.mutable_pathinfresponse();
        path_resp->set_netpath(s3_endpoint + bucket_name + "/" + input_path + "/");
        std::cout << "end_not_exist_response_async set_netpath: " << s3_endpoint + bucket_name + "/" + input_path + "/" << std::endl;
        path_resp->set_netstorepath("");
        path_resp->set_result("not exist");

        this->send_envelope(response);
    });
}

void WssSession::send_error_to_socket(const std::string& context_msg, const std::string& error_msg) {
    // Явно продлеваем жизнь текущей сессии для асинхронного колбэка
    auto self = shared_from_this();

    // Отправляем задачу в I/O поток сокета
    boost::asio::post(ws_.get_executor(), [this, self, context_msg, error_msg]() {
        ServerEnvelope response;
        response.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
        
        auto* server_resp = response.mutable_serverresp();
        
        // Формируем текст ошибки, объединяя контекст и сообщение из exception
        if (!context_msg.empty()) {
            server_resp->set_content(context_msg + ": " + error_msg);
        } else {
            server_resp->set_content(error_msg);
        }
        
        server_resp->set_status("EXCEPTION");

        // Безопасная отправка через сокет
        this->send_envelope(response);
    });
}
