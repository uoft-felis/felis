#include "entity.h"
#include "index.h"
#include "literals.h"

namespace felis {

void IndexEntity::DecodeIOVec(struct iovec *vec)
{
  auto p = (uint8_t *) vec->iov_base;
  auto key_size = vec->iov_len - 12;
  k->len = key_size;
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

mem::ParallelPool IndexEntity::pool;

void IndexEntity::InitPool()
{
  pool = mem::ParallelPool(mem::EntityPool, sizeof(IndexEntity), 16_M);
  pool.Register();
}

void RowEntity::DecodeIOVec(struct iovec *vec)
{
  auto p = (uint8_t *) vec->iov_base;

  auto key_size = *((uint16_t *)(p + 4));
  assert(key_size > 0);
  //auto value_size = vec->iov_len - 6 - key_size;

  if (k == nullptr || k->len < key_size) {
    delete k;
    k = VarStr::New(key_size);
  }
  memcpy(&rel_id, p, 4);
  memcpy((uint8_t *) k + sizeof(VarStr), p + 6, key_size);

  if (handle_ptr == nullptr)
    handle_ptr = new VHandle();
  felis::InitVersion(handle_ptr, (VarStr *) p + 6 + key_size);
}

int RowEntity::EncodeIOVec(struct iovec *vec, int max_nr_vec)
{
  if (max_nr_vec < 4)
    return 0;

  vec[0].iov_len = 4;
  vec[0].iov_base = &rel_id;
  vec[1].iov_len = 2;
  vec[1].iov_base = &(k->len);
  vec[2].iov_len = k->len;
  vec[2].iov_base = (void *) k->data;

  auto v = handle_ptr->ReadExactVersion(handle_ptr->latest_version.load());
  vec[3].iov_len = v->len;
  vec[3].iov_base = (void *) v->data;
  encoded_len = 6 + k->len + v->len;

  shipping_handle()->PrepareSend();

  return 4;
}

bool RowEntity::ShouldSkip()
{
  return (handle_ptr->size == 0) || (handle_ptr->versions[handle_ptr->capacity + handle_ptr->latest_version.load()] == kPendingValue);
}

RowEntity::RowEntity(int rel_id, VarStr *k, VHandle *handle, int slice_id)
    : alloc_coreid(mem::ParallelPool::CurrentAffinity()),
      rel_id(rel_id), k(k), handle_ptr(handle), shandle(this), slice(slice_id)
{
  // TODO: in data replay, after row_entity exists, reset(this) somewhere else
  if (handle_ptr)
    handle_ptr->row_entity.reset(this);
}

mem::ParallelPool RowEntity::pool;

void RowEntity::InitPool()
{
  pool = mem::ParallelPool(mem::EntityPool, sizeof(RowEntity), 32_M);
  pool.Register();
}

}
