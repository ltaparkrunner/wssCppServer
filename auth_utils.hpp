#pragma once
#include <string>
#include <sstream>
#include <iomanip>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <jwt-cpp/jwt.h>

namespace AuthUtils {
    
    // Безопасное хэширование SHA-256 с солью с использованием OpenSSL EVP API
    inline std::string hash_password(const std::string& password) {
        std::string salt = "fixed_salt_for_migration_123"; // Замените на вашу соль
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

    // Проверка совпадения хэшей (Аналог bcrypt.compare)
    inline bool verify_password(const std::string& password, const std::string& stored_hash) {
        return hash_password(password) == stored_hash;
    }

    // Создание JWT токена
    inline std::string generate_token(const std::string& user_id, const std::string& secret) {
        auto token = jwt::create()
            .set_issuer("picture_server")
            .set_type("JWS")
            .set_payload_claim("id", jwt::claim(user_id))
            .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{4})
            .sign(jwt::algorithm::hs256{secret});
        return token;
    }
}
