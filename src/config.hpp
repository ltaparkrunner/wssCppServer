// src/config.hpp
#pragma once
#include <cstdlib>
#include <string>

struct Config {
    std::string mongo_url;
    std::string s3_endpoint;
    std::string s3_access_key;
    std::string s3_secret_key;
    std::string s3_bucket;
    std::string jwt_secret;
    int s3_ref_expires;

    static Config load() {
        const char* expires_env = std::getenv("S3_REF_EXPIRES");
        int expires_val = expires_env ? std::atoi(expires_env) : 3600;

        return Config{
            get_env("MONGO_URL", "mongodb://localhost:27017/images_db"),
            get_env("S3_ENDPOINT", "http://localhost:9000/"),
            get_env("S3_ACCESS_KEY", "admin"),
            get_env("S3_SECRET_KEY", "password123"),
            get_env("S3_BUCKET", "images"),
            get_env("JWT_SECRET", "secret_key"),
            expires_val
        };
    }

private:
    static std::string get_env(const char* key, const std::string& default_val) {
        const char* val = std::getenv(key);
        return val ? std::string(val) : default_val;
    }
};
