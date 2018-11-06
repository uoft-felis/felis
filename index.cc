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

#include "felis_probes.h"

using util::Instance;

namespace felis {

std::map<std::string, Checkpoint *> Checkpoint::impl;

void IndexEntity::DecodeIOVec(struct iovec *vec)
{
  auto p = (uint8_t *) vec->iov_base;
  auto key_size = vec->iov_len - 12;
  if (k == nullptr || k->len < key_size) {
    delete k;
    k = VarStr::New(key_size);
  }
  memcpy(&rel_id, p, 4);
  memcpy((uint8_t *) k + sizeof(VarStr), p + 4, key_size);
  memcpy(&handle_ptr, p + key_size + 4, 8); // This is a pointer on the original machine though.
}

void IndexEntity::EncodeIOVec(struct iovec *vec)
{
  auto p = (uint8_t *) vec->iov_base;
  vec->iov_len = k->len + 12;
  memcpy(p, &rel_id, 4);
  memcpy(p + 4, k->data, k->len);
  memcpy(p + 4 + k->len, &handle_ptr, 8);
}

IndexEntity::~IndexEntity()
{
  delete k;
}

void IndexShipmentReceiver::Run()
{
  IndexEntity ent;
  auto &mgr = Instance<RelationManager>();
  while (Receive(&ent)) {
    // TODO: multi-thread this?
    auto &rel = mgr[ent.rel_id];
    rel.InsertOrDefault(ent.k, [&ent]() { return ent.handle_ptr; });
  }
}

}
