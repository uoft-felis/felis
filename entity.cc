#include "entity.h"

namespace felis {

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

int IndexEntity::EncodeIOVec(struct iovec *vec, int max_nr_vec)
{
  if (max_nr_vec < 3)
    return 0;

  vec[0].iov_len = 4;
  vec[0].iov_base = &rel_id;
  vec[1].iov_len = k->len;
  vec[1].iov_base = (void *) k->data;
  vec[2].iov_len = 8;
  vec[2].iov_base = &handle_ptr;

  encoded_len = 12 + k->len;

  return 3;
}

IndexEntity::~IndexEntity()
{
  delete k;
}

void RowEntity::DecodeIOVec(struct iovec *vec)
{
  // TODO
}

int RowEntity::EncodeIOVec(struct iovec *vec, int max_nr_vec)
{
  if (max_nr_vec < 3)
    return 0;

  vec[0].iov_len = 4;
  vec[0].iov_base = &rel_id;
  vec[1].iov_len = k->len;
  vec[1].iov_base = (void *) k->data;

  auto v = handle_ptr->ReadExactVersion(this->newest_version.load());
  vec[2].iov_len = v->len;
  vec[2].iov_base = (void *) v->data;
  encoded_len = 4 + k->len + v->len;

  shipping_handle()->PrepareSend();

  return 3;
}

bool RowEntity::ShouldSkip()
{
  return (handle_ptr->size == 0) || (handle_ptr->versions[handle_ptr->capacity + this->newest_version.load()] == kPendingValue);
}

RowEntity::RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id)
    : rel_id(rel_id), k(k), handle_ptr(handle), shandle(this), slice(slice_id)
{
  // TODO: in data replay, after row_entity exists, reset(this) somewhere else
  if (handle_ptr)
    handle_ptr->row_entity.reset(this);
}

mem::Pool RowEntity::pool;

void RowEntity::InitPools()
{
  pool.move(mem::BasicPool(mem::RowEntityPool, sizeof(RowEntity), 64 << 20));
}

void RowShipmentReceiver::Run()
{
// clear the affinity
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  RowEntity ent;

  logger->info("New Row Shipment has arrived!");
  PerfLog perf;
  int count = 0;
  while (Receive(&ent)) {
    // TODO: really do something about the shipment
    count++;
  }
  logger->info("Row Shipment processing finished, received {} RowEntities", count);
  perf.End();
  perf.Show("Processing takes");
  sock->Close();

}

}
