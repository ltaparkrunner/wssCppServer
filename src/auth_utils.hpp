#pragma once
#include <string>
#include <jwt-cpp/jwt.h>
#include <bcrypt.h> // Системная библиотека libbcrypt

namespace AuthUtils {
    // Хэширование пароля (аналог userSchema.pre('save'))
    std::string hash_password(const std::string& password) {
        char salt[BCRYPT_HASHSIZE];
        char hash[BCRYPT_HASHSIZE];
        
        if (bcrypt_gensalt(10, salt) != 0) throw std::runtime_error("Salt generation failed");
        if (bcrypt_hashpw(password.c_str(), salt, hash) != 0) throw std::runtime_error("Hashing failed");
        
        return std::string(hash);
    }

    // Проверка пароля (аналог comparePassword)
    bool verify_password(const std::string& password, const std::string& hash) {
        return bcrypt_checkpw(password.c_str(), hash.c_str()) == 0;
    }

    // Создание токена (аналог jwt.sign)
    std::string generate_token(const std::string& user_id, const std::string& secret) {
        auto token = jwt::create()
            .set_issuer("picture_server")
            .set_type("JWS")
            .set_payload_claim("id", jwt::claim(user_id))
            .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{4})
            .sign(jwt::algorithm::hs256{secret});
        return token;
    }
}
