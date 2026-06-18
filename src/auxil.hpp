#pragma once
#include <string>
#include <map>
#include <sstream>
#include <iostream>
#include <iomanip>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <filesystem>

// Декодирование URL-символов (заменяет %20 на пробелы, %3A на двоеточия и т.д.)
inline std::string url_decode(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (std::size_t i = 0; i < in.size(); ++i) {
        if (in[i] == '%') {
            if (i + 2 < in.size()) {
                int value = 0;
                std::istringstream is(in.substr(i + 1, 2));
                if (is >> std::hex >> value) {
                    out += static_cast<char>(value);
                    i += 2;
                }
            }
        } else if (in[i] == '+') {
            out += ' '; // В urlencoded + означает пробел
        } else {
            out += in[i];
        }
    }
    return out;
}

// Извлечение параметров из x-www-form-urlencoded тела запроса
inline std::map<std::string, std::string> parse_form_body(const std::string& body) {
    std::map<std::string, std::string> params;
    std::istringstream ss(body);
    std::string token;
    while (std::getline(ss, token, '&')) {
        auto edge = token.find('=');
        if (edge != std::string::npos) {
            std::string key = url_decode(token.substr(0, edge));
            std::string value = url_decode(token.substr(edge + 1));
            params[key] = value;
        }
    }
    return params;
}

// Аналог Node.js функции prepareFilename
struct FilenameInfo {
    std::string unique_name;
    std::string ext;
};

FilenameInfo prepare_filename(const std::string& original_name);
