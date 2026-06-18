#pragma once
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <random>
#include <iomanip>

namespace AuxHandler {

    // Внутренняя функция очистки отдельного сегмента (Аналог библиотеки sanitize-filename)
    inline std::string sanitize_segment(const std::string& segment) {
        std::string result = segment;
        
        // 1. Удаляем запрещенные в именах файлов символы: / \ ? * : | < > "
        result.erase(std::remove_if(result.begin(), result.end(), [](unsigned char c) {
            return c == '/' || c == '\\' || c == '?' || c == '*' || 
                   c == ':' || c == '|' || c == '<' || c == '>' || c == '"';
        }), result.end());

        // 2. Переводим в нижний регистр (как в вашем JS: .toLowerCase())
        std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) {
            return std::tolower(c);
        });

        // 3. Заменяем один или несколько пробелов на один дефис (Аналог: .replace(/\s+/g, '-'))
        std::string space_cleaned;
        bool in_space = false;
        for (char c : result) {
            if (std::isspace(static_cast<unsigned char>(c))) {
                if (!in_space) {
                    space_cleaned += '-';
                    in_space = true;
                }
            } else {
                space_cleaned += c;
                in_space = false;
            }
        }
        return space_cleaned;
    }

    // Полная копия функции sanitizeToPath из вашего JS-кода
    inline std::string sanitize_to_path(const std::string& input) {
        if (input.empty()) return "";

        // Шаг 1: Заменяем обратные слеши на прямые
        std::string path_str = input;
        std::replace(path_str.begin(), path_str.end(), '\\', '/');

        // Шаг 2 и 3: Разбиваем по слешам (это автоматически схлопывает кратные слеши 
        // и игнорирует пустые сегменты в начале и конце — аналог тримминга и split('/'))
        std::vector<std::string> segments;
        std::stringstream ss(path_str);
        std::string item;
        
        while (std::getline(ss, item, '/')) {
            if (!item.empty()) {
                // Шаг 4: Очищаем каждый отдельный сегмент
                std::string sanitized = sanitize_segment(item);
                if (!sanitized.empty()) {
                    segments.push_back(sanitized);
                }
            }
        }

        // Собираем обратно через '/' (Аналог .join('/'))
        std::string final_path;
        for (size_t i = 0; i < segments.size(); ++i) {
            final_path += segments[i];
            if (i != segments.size() - 1) {
                final_path += '/';
            }
        }
        return final_path;
    }

    // Генератор UUID v4 (RFC 4122)
    inline std::string generate_uuid() {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<> dis(0, 15);
        static std::uniform_int_distribution<> dis2(8, 11);

        std::stringstream ss;
        ss << std::hex;
        for (int i = 0; i < 8; i++) ss << dis(gen);
        ss << "-";
        for (int i = 0; i < 4; i++) ss << dis(gen);
        ss << "-4"; // версия 4
        for (int i = 0; i < 3; i++) ss << dis(gen);
        ss << "-";
        ss << dis2(gen);
        for (int i = 0; i < 3; i++) ss << dis(gen);
        ss << "-";
        for (int i = 0; i < 12; i++) ss << dis(gen);
        return ss.str();
    }

    // Полная копия функции prepareFilename
    // Возвращает структуру {uniqueName, ext}
    // struct FilenameResult {
    //     std::string uniqueName;
    //     std::string ext;
    // };

    // inline FilenameResult prepare_filename(const std::string& original_name) {
    //     std::string name_only = original_name;
    //     std::string ext = "";

    //     // Извлекаем расширение (Аналог path.extname)
    //     size_t dot_pos = original_name.find_last_of(".");
    //     if (dot_pos != std::string::npos && dot_pos != 0) {
    //         name_only = original_name.substr(0, dot_pos);
    //         ext = original_name.substr(dot_pos); // Сохраняет точку (например, ".png")
    //     }

    //     // Очищаем имя (Аналог safeName = sanitize(nameOnly)...)
    //     std::string safe_name = sanitize_segment(name_only);

    //     // Собираем финальное имя: ${uuidv4()}-${safeName}${ext}
    //     std::string unique_name = generate_uuid() + "-" + safe_name + ext;

    //     return FilenameResult{unique_name, ext};
    // }
}
