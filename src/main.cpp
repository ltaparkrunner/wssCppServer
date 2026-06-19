#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <nlohmann/json.hpp>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h> 

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/oid.hpp>
#include <mongocxx/instance.hpp>

#include "config.hpp"
#include "database.hpp"
#include "auth_utils.hpp"
#include "aux_handler.hpp"
#include "image.pb.h"
#include "auxil.hpp"
#include "wssSession.hpp"
#include "httpServerSession.hpp"

namespace asio = boost::asio;
namespace ssl = boost::asio::ssl;
namespace beast = boost::beast;
namespace http = boost::beast::http;
namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;

inline std::string url_decode(const std::string& in);
inline std::map<std::string, std::string> parse_form_body(const std::string& body);

// Сетевой слушатель портов
class NetworkListener : public std::enable_shared_from_this<NetworkListener> {
    asio::io_context& ioc_;
    ssl::context& ctx_;
    tcp::acceptor acceptor_;
    Database& db_;
    Aws::S3::S3Client& s3_client_;
    Config& cfg_;
    bool is_auth_;

public:
    NetworkListener(asio::io_context& ioc, ssl::context& ctx, tcp::endpoint ep, Database& db, Aws::S3::S3Client& s3, Config& cfg, bool is_auth)
        : ioc_(ioc), ctx_(ctx), acceptor_(asio::make_strand(ioc)), db_(db), s3_client_(s3), cfg_(cfg), is_auth_(is_auth) {
        acceptor_.open(ep.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
    }

    void start() { do_accept(); }

private:
    void do_accept() {
        acceptor_.async_accept(asio::make_strand(ioc_), beast::bind_front_handler(&NetworkListener::on_accept, shared_from_this()));
    }

    void on_accept(beast::error_code ec, tcp::socket socket) {
        if (!ec) {
            std::make_shared<HttpServerSession>(std::move(socket), ctx_, db_, s3_client_, cfg_, is_auth_)->start();
        }
        do_accept();
    }
};

int main() {
    mongocxx::instance instance{}; 
    auto cfg = Config::load();
    Database db(cfg.mongo_url);

    Aws::SDKOptions options;
    Aws::InitAPI(options);
//    Aws::S3::S3Client s3_client;

    // А. Настраиваем эндпоинт (например, http://localhost:9000/)
    Aws::Client::ClientConfiguration client_config;
    client_config.endpointOverride = cfg.s3_endpoint; 
    std::cout << "client_config.endpointOverride" << client_config.endpointOverride << std::endl;
    // client_config.enableVirtualAddressing = false; 

    // Б. Передаем ключи доступа из config.hpp
    Aws::Auth::AWSCredentials credentials;
    credentials.SetAWSAccessKeyId(cfg.s3_access_key);
    credentials.SetAWSSecretKey(cfg.s3_secret_key);

    // В. Создаем клиент с отключенной виртуальной адресацией (useVirtualAddressing = false)
    Aws::S3::S3Client s3_client(
        credentials, 
        client_config, 
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, 
        false // <- ИСПРАВЛЕНИЕ: Это принудительно включает Path-Style адресацию для MinIO/LocalStack
    );

    const int threads_count = std::max(1u, std::thread::hardware_concurrency());
    asio::io_context ioc{threads_count};

    ssl::context ctx{ssl::context::tlsv12_server};
    ctx.use_certificate_chain_file("cert.pem");
    ctx.use_private_key_file("key.pem", ssl::context::pem);

    auto wss_ep = tcp::endpoint{asio::ip::make_address("0.0.0.0"), 8080};
    auto auth_ep = tcp::endpoint{asio::ip::make_address("0.0.0.0"), 8081};

    std::make_shared<NetworkListener>(ioc, ctx, wss_ep, db, s3_client, cfg, false)->start();
    std::make_shared<NetworkListener>(ioc, ctx, auth_ep, db, s3_client, cfg, true)->start();

    std::vector<std::thread> threads;
    for (int i = 0; i < threads_count - 1; ++i) {
        threads.emplace_back([&ioc]() { ioc.run(); });
    }
    std::cout << "C++ Servers started. WSS on 8080, Auth REST on 8081." << std::endl;
    ioc.run();

    Aws::ShutdownAPI(options);
    return 0;
}
