#pragma once
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <memory>

class Database {
    std::unique_ptr<mongocxx::client> client_;
    std::string db_name_;

public:
    Database(const std::string& uri_str) {
        mongocxx::uri uri{uri_str};
        // ИСПРАВЛЕНО: добавлен символ подчеркивания, соответствующий объявлению члена класса
        client_ = std::make_unique<mongocxx::client>(uri);
        db_name_ = uri.database();
        if(db_name_.empty()) db_name_ = "images_db";
    }

    mongocxx::database get_db() {
        return (*client_)[db_name_];
    }
};