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

int RowEntity::EncodeIOVec(struct iovec *vec, int max_nr_vec)
{
  if (max_nr_vec < 3)
    return 0;

  vec[0].iov_len = 4;
  vec[0].iov_base = &rel_id;
  vec[1].iov_len = k->len;
  vec[1].iov_base = (void *) k->data;
  ulong n_ver = this->newest_version.load();
  vec[2].iov_len = handle_ptr->ReadWithVersion(n_ver)->len;
  vec[2].iov_base = (void *) handle_ptr->ReadWithVersion(n_ver)->data;

  encoded_len = 4 + k->len + handle_ptr->ReadWithVersion(n_ver)->len;

  shipping_handle()->PrepareSend();

  return 3;
}

RowEntity::RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id)
    : rel_id(rel_id), k(k), handle_ptr(handle), shandle(this), slice(slice_id)
{
  handle_ptr->row_entity.reset(this);
}

mem::Pool RowEntity::pool;

void RowEntity::InitPools()
{
  pool.move(mem::Pool(sizeof(RowEntity), 64 << 20));
}

void RowShipmentReceiver::Run()
{
  TBD();
}

}