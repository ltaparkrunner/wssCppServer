cmake_minimum_required(VERSION 3.22)
project(picture_server CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Находим системные потоки операционной системы (pthread)
find_package(Threads REQUIRED)

# Находим Boost и его компоненты
find_package(Boost REQUIRED COMPONENTS thread)

# Находим остальные зависимости из vcpkg
find_package(protobuf REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(jwt-cpp REQUIRED)
find_package(bsoncxx REQUIRED)
find_package(mongocxx REQUIRED)

# Находим AWS SDK С++ в CONFIG режиме (стандарт vcpkg)
find_package(AWSSDK REQUIRED COMPONENTS s3 core CONFIG)

# 1. Объявляем исполняемый файл сервера
add_executable(picture_server src/main.cpp)

# 2. Генерируем C++ файлы из вашей схемы image.proto
protobuf_generate(
    TARGET picture_server
    PROTOS image.proto
    LANGUAGE cpp
)

# Подключаем зависимости к бинарнику
target_link_libraries(picture_server PRIVATE
    Boost::headers
    Boost::thread
    Threads::Threads
    protobuf::libprotobuf
    nlohmann_json::nlohmann_json
    jwt-cpp::jwt-cpp
    
    # Кроссплатформенный способ линковки Mongo из vcpkg:
    $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>
    $<IF:$<TARGET_EXISTS:mongo::bsoncxx_static>,mongo::bsoncxx_static,mongo::bsoncxx_shared>
    
    # Правильные плоские имена импортируемых таргетов AWS SDK под Linux в vcpkg
    aws-cpp-sdk-s3
    aws-cpp-sdk-core
)

# Инструкция для Docker: куда скопировать бинарник при сборке
install(TARGETS picture_server DESTINATION .)
