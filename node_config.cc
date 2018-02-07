#include <fstream>
#include <iterator>
#include "json11/json11.hpp"
#include "node_config.h"

namespace dolly {

NodeConfiguration *NodeConfiguration::instance = nullptr;

static const std::string kNodeConfiguration = "nodes.json";


static void ParseNodeConfig(NodeConfiguration::NodeConfig *config, json11::Json json)
{
  auto json_arr = json.array_items();
  for (auto &json_obj: json_arr) {
    auto &json_map = json_obj.object_items();
    std::string host = json_map.find("host")->second.string_value();
    int port = json_map.find("port")->second.int_value();
  }
}

NodeConfiguration::NodeConfiguration()
{
  const char *strid = getenv("DOLLY_NODE_ID");

  if (!strid || (id = std::stoi(std::string(strid))) <= 0) {
    std::abort();
  }

  std::ifstream fin(kNodeConfiguration);
  std::string conf_text{std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>()};
  std::string err;
  json11::Json conf_doc = json11::Json::parse(conf_text, err);

  if (!err.empty()) {
    std::abort();
  }
  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    int idx = std::stoi(it->first);
    auto *config = &all_config.at(idx);
    config->id = idx;
    ParseNodeConfig(config, it->second);
  }
}

}
