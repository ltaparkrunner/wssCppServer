#pragma once
#include <memory>
#include <string>
#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>

#include <iostream>
#include <string>
#include <regex>
#include <chrono>
#include <future>
#include <nlohmann/json.hpp>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <mongocxx/instance.hpp>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>

#include "image.pb.h"
#include "config.hpp"
#include "database.hpp"
#include "aux_handler.hpp"

namespace asio = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;

inline std::string url_decode(const std::string& in);
inline std::map<std::string, std::string> parse_form_body(const std::string& body);

// --- СЛОЙ 1: WEB SOCKET SECURE SESSION (Порт 8080) ---
class WssSession : public std::enable_shared_from_this<WssSession> {
    // Настраиваем тип WebSocket-стрима на точное совпадение с типом из HTTP-сессии
    websocket::stream<beast::ssl_stream<tcp::socket>> ws_;
    beast::flat_buffer buffer_;
    Database& db_;
    Aws::S3::S3Client& s3_client_;
    Config& cfg_;
    std::string user_id_;
    http::request<http::string_body> upgrade_req_;

public:
    // Конструктор принимает УЖЕ ГОТОВЫЙ, полностью рабочий SSL-стрим,
    // в котором Qt-клиент уже успешно прошел шифрование!
    WssSession(beast::ssl_stream<tcp::socket> ssl_stream, Database& db, Aws::S3::S3Client& s3, Config& cfg, std::string uid)
        : ws_(std::move(ssl_stream)), db_(db), s3_client_(s3), cfg_(cfg), user_id_(uid) {}

    void start(http::request<http::string_body> req) {
        upgrade_req_ = std::move(req);
        
        // Отключаем таймауты Beast на время хэндшейка сокетов
        //  beast::get_lowest_layer(ws_).expires_never();
        
        // Запускаем асинхронный WebSocket Handshake прямо поверх живого SSL
        ws_.async_accept(upgrade_req_, beast::bind_front_handler(&WssSession::on_accept, shared_from_this()));
    }

private:
    void on_accept(beast::error_code ec) {
        if (ec) {
            std::cerr << "WSS Accept Error: " << ec.message() << std::endl;
            return;
        }
        do_read();
    }

    void do_read() {
        buffer_.clear();
        ws_.async_read(buffer_, beast::bind_front_handler(&WssSession::on_read, shared_from_this()));
    }

    void on_read(beast::error_code ec, std::size_t bytes_transferred) {
        boost::ignore_unused(bytes_transferred);
        if (ec == websocket::error::closed) return;
        if (ec) return;

        if (ws_.got_binary()) {
            std::string raw_data = beast::buffers_to_string(buffer_.data());
            ClientEnvelope envelope;
            if (envelope.ParseFromString(raw_data)) {
                dispatch_protobuf_message(envelope);
            }
        }
    }

    void dispatch_protobuf_message(const ClientEnvelope& envelope) {
/*


                        if(envelope.reqUserBuckets){
                            // console.log("Received bucket list request from user: ", req.user ? req.user.id : "Unknown");
                            await handleGetUserBucket(ws, envelope.reqUserBuckets, s3Client, req.user ? req.user.id : "Unknown");
                        }                   //      envelope.content
                        if(envelope.addFile){
                            // async function handleAddFile(msg, root, s3Client, BaseMessage, ws)
                            await handleAddFile(ws, envelope.addFile, s3Client, req.user.id);
                        }


*/
        if (envelope.type() != ClientEnvelope_Type_CLIENT_MESSAGE) {
            std::cerr << "Received non-client message envelope." << std::endl;
            send_error("Unhandled or unknown envelope type.");
            return;
        }
        if(envelope.has_requserbuckets()) {
            std::cout << "Received UserBucketsRequest from user " << user_id_ << std::endl;
            handle_user_bucket(envelope.requserbuckets());            
        } 
        if (envelope.has_addfile()) {
            std::cout << "Received AddFileRequest from user " << user_id_ << std::endl;
            handle_add_file(envelope.addfile());
        } 
        else if (envelope.has_listrequest()) {
            std::cout << "Received FilesFoldersListRequest from user " << user_id_ << std::endl;
            handle_list_request(envelope.listrequest());
        } 
        else if (envelope.has_deletefile()) {
            std::cout << "Received DeleteFileRequest from user " << user_id_ << std::endl;
            handle_delete_file(envelope.deletefile());
        } 
        else if (envelope.has_filesidsrequest()) {
            std::cout << "Received FilesIdsRequest from user " << user_id_ << std::endl;
            handle_files_ids_request(envelope.filesidsrequest());
        } 
        else if (envelope.has_pathinfrequest()) {
            std::cout << "Received PathInfoRequest from user " << user_id_ << std::endl;
            handle_path_inf_request(envelope.pathinfrequest());
        } 
        else {
            std::cerr << "Received unknown or empty client message from user " << user_id_ << std::endl;
            send_error("Empty or unhandled packet action.");
        }
    } 
                               // reqUserBuckets    
    void handle_user_bucket(const BucketsRequest& req);
                            // addFile
    void handle_add_file(const AddFileRequest& req);  // { boost::ignore_unused(req); }
    void handle_list_request(const FilesFoldersListRequest& req);   // { boost::ignore_unused(req); }
    void handle_delete_file(const DeleteFileRequest& req);  // { boost::ignore_unused(req); }
    void handle_files_ids_request(const FilesIds& req);     // { boost::ignore_unused(req); }
    void handle_path_inf_request(const PathInfoRequest& req);   // { boost::ignore_unused(req); }
    void send_not_exist_response_async(const std::string& input_path, const std::string& s3_endpoint, const std::string& bucket_name);

    void send_envelope(const ServerEnvelope& env) {
        std::string out;
        if (env.SerializeToString(&out)) {
            ws_.binary(true);
            auto shared_data = std::make_shared<std::string>(std::move(out));
            ws_.async_write(asio::buffer(*shared_data), [self = shared_from_this(), shared_data](beast::error_code ec, std::size_t) {
                if (!ec) self->do_read();
            });
        }
    }

    void send_error(const std::string& msg) {
        ServerEnvelope resp;
        resp.set_type(ServerEnvelope_Type_SERVER_MESSAGE);
        ServerResponse* s_res = resp.mutable_serverresp();
        s_res->set_content(msg);
        s_res->set_status("error");
        send_envelope(resp);
    }
};

