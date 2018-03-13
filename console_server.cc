#include <boost/network/protocol/http/server.hpp>
#include <boost/network/protocol/http/response.hpp>
#include <iostream>
#include <thread>
#include "console.h"
#include "util.h"

namespace http = boost::network::http;

namespace dolly {

class ConsoleHandler;
using ConsoleServer = http::server<ConsoleHandler>;
using ConsoleRequest = ConsoleServer::request;
using ConsoleConnection = ConsoleServer::connection;
using ConsoleConnectionPtr = ConsoleServer::connection_ptr;
using ConsoleServerString = ConsoleServer::string_type;
using ConsoleResponseHeader = ConsoleServer::response_header;

class ConsoleHandler {
 public:
  void operator()(ConsoleRequest const &request, ConsoleConnectionPtr connection) {
    // ConsoleServerString src = source(request);
    auto &console = util::Instance<Console>();
    connection->set_status(ConsoleConnection::ok);
    ConsoleResponseHeader hdrs[] = {{"Connection", "close"}};
    connection->set_headers(boost::make_iterator_range(hdrs, hdrs + 1));
    connection->write(console.HandleAPI(request.destination));
  }
  void log(const ConsoleServerString& message) {
    std::cerr << "ERROR: " << message << std::endl;
  }
};


void RunConsoleServer(std::string netmask, std::string service_port)
{
  auto t = std::thread([=]() {
      ConsoleHandler handler;
      ConsoleServer::options opt(handler);
      ConsoleServer _(opt.address(netmask).port(service_port));
      _.run();
    });
  t.detach();
}

}
