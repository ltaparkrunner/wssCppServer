#pragma once
#include <string>
#include <sstream>
#include <iomanip>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <nlohmann/json.hpp>
#include <jwt-cpp/jwt.h>

namespace AuthUtils {
    
    inline std::string hash_password(const std::string& password) {
        std::string salt = "fixed_salt_for_migration_123";
        std::string salted_password = password + salt;

        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int hash_len = 0;

        EVP_MD_CTX* context = EVP_MD_CTX_new();
        if(context != nullptr) {
            if(EVP_DigestInit_ex(context, EVP_sha256(), nullptr) &&
               EVP_DigestUpdate(context, salted_password.c_str(), salted_password.size()) &&
               EVP_DigestFinal_ex(context, hash, &hash_len)) {
                EVP_MD_CTX_free(context);
            } else {
                EVP_MD_CTX_free(context);
                throw std::runtime_error("OpenSSL hashing failed");
            }
        }

        std::stringstream ss;
        for(unsigned int i = 0; i < hash_len; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
        }
        return ss.str();
    }

    inline bool verify_password(const std::string& password, const std::string& stored_hash) {
        return hash_password(password) == stored_hash;
    }

    // Полностью совместимый способ генерации JWT токена без использования jwt::create
    inline std::string generate_token(const std::string& user_id, const std::string& secret) {
        // Создаем чистый билдер на базе трейтов nlohmann_json
        jwt::builder<jwt::default_clock, jwt::traits::nlohmann_json> b;
        
        auto token = b.set_issuer("picture_server")
            .set_type("JWS")
            .set_payload_claim("id", nlohmann::json(user_id))
            .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{4})
            .sign(jwt::algorithm::hs256{secret});
        return token;
    }
}
