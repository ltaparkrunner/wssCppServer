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
