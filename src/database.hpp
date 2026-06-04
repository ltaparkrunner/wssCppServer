#pragma once
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>
#include <memory>
#include <string>

class Database {
    std::unique_ptr<mongocxx::client> client_;
    std::string db_name_;

public:
    Database(const std::string& uri_str) {
        // Защита парсера: отсекаем имя БД из URI, если оно там присутствует
        std::string clean_uri = uri_str;
        size_t last_slash = uri_str.find_last_of('/');
        
        // Если слэш найден после протокола mongodb:// (индекс 10)
        if (last_slash != std::string::npos && last_slash > 10) {
            clean_uri = uri_str.substr(0, last_slash);
            db_name_ = uri_str.substr(last_slash + 1);
        }

        if (db_name_.empty()) {
            db_name_ = "images_db";
        }

        // Теперь URI гарантированно валиден для mongocxx::client
        mongocxx::uri uri{clean_uri};
        client_ = std::make_unique<mongocxx::client>(uri);
    }

    mongocxx::database get_db() {
        return (*client_)[db_name_];
    }
};
