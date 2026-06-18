#include "auxil.hpp"

FilenameInfo prepare_filename(const std::string& original_name) {
    std::filesystem::path p(original_name);
    std::string ext = p.extension().string(); // Получаем расширение, например, ".jpg"
    if (!ext.empty() && ext.front() == '.') {
        ext = ext.substr(1); // Убираем точку для записи в БД типа {type: "jpg"}
    }
    
    // Генерируем случайный UUID v4
    boost::uuids::random_generator gen;
    boost::uuids::uuid u = gen();
    std::string uuid_str = boost::uuids::to_string(u);
    
    // Формируем уникальное имя файла: uuid.jpg
    std::string unique_name = ext.empty() ? uuid_str : (uuid_str + "." + ext);
    
    return {unique_name, ext};
}

std::string sanitize(const std::string& segment) {
    std::string result = segment;

    // 1. Remove control characters & blacklisted filesystem symbols (\ / ? < > : * | ")
    result.erase(std::remove_if(result.begin(), result.end(), [](unsigned char c) {
        return (c < 32 || c == 127) || // Control characters
               (c == '\\' || c == '/' || c == '?' || c == '<' || c == '>' || 
                c == ':'  || c == '*' || c == '|' || c == '"');
    }), result.end());

    // 2. Remove trailing spaces and trailing periods (Windows compatibility)
    while (!result.empty() && (result.back() == ' ' || result.back() == '.')) {
        result.pop_back();
    }

    // 3. Block Unix relative lookups and Windows reserved names
    std::string upper = result;
    std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
    
    const std::vector<std::string> reserved = {
        ".", "..", "CON", "PRN", "AUX", "NUL", 
        "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
        "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
    };
    
    if (std::find(reserved.begin(), reserved.end(), upper) != reserved.end()) {
        return ""; // Invalidate the segment entirely
    }

    // 4. Truncate to maximum standard filename size (255 bytes)
    if (result.length() > 255) {
        result = result.substr(0, 255);
    }

    return result;
}

std::string sanitizeToPath(const std::string& input) {
    std::vector<std::string> segments;
    std::string current_segment;

    for (char c : input) {
        // Normalize backslashes to forward slashes
        if (c == '\\') c = '/';

        if (c == '/') {
            if (!current_segment.empty()) {
                std::string sanitized = sanitize(current_segment);
                if (!sanitized.empty()) {
                    segments.push_back(sanitized);
                }
                current_segment.clear();
            }
        } else {
            current_segment.push_back(c);
        }
    }
    
    // Don't forget the final segment if the string didn't end with a slash
    if (!current_segment.empty()) {
        std::string sanitized = sanitize(current_segment);
        if (!sanitized.empty()) {
            segments.push_back(sanitized);
        }
    }

    // Join with '/'
    std::string result;
    for (size_t i = 0; i < segments.size(); ++i) {
        result += segments[i];
        if (i < segments.size() - 1) result += "/";
    }
    return result;
}
