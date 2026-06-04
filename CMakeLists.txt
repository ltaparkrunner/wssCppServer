cmake_minimum_required(VERSION 3.22)
project(picture_server CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS thread)
find_package(protobuf REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(jwt-cpp REQUIRED)
find_package(bsoncxx REQUIRED)
find_package(mongocxx REQUIRED)
find_package(AWSSDK REQUIRED COMPONENTS s3 core CONFIG)

add_executable(picture_server src/main.cpp)

protobuf_generate(
    TARGET picture_server
    PROTOS image.proto
    LANGUAGE cpp
)

# Добавляем явные пути поиска заголовочных файлов для таргета picture_server
target_include_directories(picture_server PRIVATE 
    ${CMAKE_CURRENT_BINARY_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_include_directories(picture_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(picture_server PRIVATE
    Boost::headers
    Boost::thread
    Threads::Threads
    protobuf::libprotobuf
    nlohmann_json::nlohmann_json
    jwt-cpp::jwt-cpp
    $<IF:$<TARGET_EXISTS:mongo::mongocxx_static>,mongo::mongocxx_static,mongo::mongocxx_shared>
    $<IF:$<TARGET_EXISTS:mongo::bsoncxx_static>,mongo::bsoncxx_static,mongo::bsoncxx_shared>
    aws-cpp-sdk-s3
    aws-cpp-sdk-core
)

install(TARGETS picture_server DESTINATION .)
