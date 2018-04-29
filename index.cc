#include <fstream>
#include <limits>
#include <streambuf>
#include <iomanip>
#include <dlfcn.h>
#include "index.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"
#include "json11/json11.hpp"
#include "gopp/gopp.h"

#include "dolly_probes.h"

using util::Instance;

namespace dolly {

RelationManagerBase::RelationManagerBase()
{
  std::string err;
  std::ifstream fin("relation_map.json");

  // Wow...I thought I will never encounter most-vexing parse for the rest of my
  // life....and today, I encountered two of them on Clang!
  // - Mike
  std::string conf_text {
    std::istreambuf_iterator<char>(fin), std::istreambuf_iterator<char>() };

  json11::Json conf_doc = json11::Json::parse(conf_text, err);
  if (!err.empty()) {
    logger->critical(err);
    logger->critical("Cannot read relation id map configuration!");
    std::abort();
  }

  auto json_map = conf_doc.object_items();
  for (auto it = json_map.begin(); it != json_map.end(); ++it) {
    // assume it->second is int!
    // TODO: validations!
    relation_id_map[it->first] = it->second.int_value();
    logger->info("relation name {} map to id {}", it->first,
		 relation_id_map[it->first]);
  }
}

std::map<std::string, Checkpoint *> Checkpoint::impl;

}
