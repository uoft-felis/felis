#include "promise.h"
#include "gopp/gopp.h"
#include "node_config.h"
#include <queue>
#include "util.h"

using util::Instance;

namespace dolly {

PromiseRoutinePool *PromiseRoutinePool::Create(size_t size)
{
  PromiseRoutinePool *p = (PromiseRoutinePool *) malloc(sizeof(PromiseRoutinePool) + size);
  p->mem_size = size;
  p->refcnt = 0;
  p->input_ptr = nullptr;
  return p;
}

void PromiseRoutine::UnRef()
{
  if (!pool->IsManaging(input.data))
    free((void *) input.data);

  if (pool->refcnt.fetch_sub(1) == 1) {
    free(pool); // which also free this
  }
}

void PromiseRoutine::UnRefRecursively()
{
  if (next) {
    auto handlers = next->handlers;
    if (next->UnRef() == 0) {
      for (auto child: handlers) {
        child->UnRefRecursively();
      }
    }
  }
  UnRef();
}

PromiseRoutine *PromiseRoutine::CreateWithDedicatePool(size_t capture_len)
{
  auto *p = PromiseRoutinePool::Create(sizeof(PromiseRoutine) + capture_len);
  auto *r = (PromiseRoutine *) p->mem;
  r->capture_len = capture_len;
  r->capture_data = p->mem + sizeof(PromiseRoutine);
  r->pool = p;
  r->Ref();
  return r;
}

PromiseRoutine *PromiseRoutine::CreateFromBufferedPool(PromiseRoutinePool *rpool)
{
  uint8_t *p = rpool->mem;
  uint16_t len;
  memcpy(&len, p, 2);
  rpool->input_ptr = p + 2;
  p += util::Align(2 + len, 8);
  auto r = (PromiseRoutine *) p;
  r->DecodeTree(rpool);
  return r;
}

size_t PromiseRoutine::TreeSize() const
{
  return util::Align(2 + input.len, 8) + NodeSize();
}

void PromiseRoutine::EncodeTree(uint8_t *p)
{
  // Format: input data are placed before the tree root. Every tree node
  // addressess are aligned to 8 bytes. (C standards)
  uint8_t *start = p;
  memcpy(p, &input.len, 2);
  memcpy(p + 2, input.data, input.len);
  p += util::Align(2 + input.len, 8);
  EncodeNode(p);
  auto root = (PromiseRoutine *) p;
  root->input.data = start;
}

void PromiseRoutine::DecodeTree(PromiseRoutinePool *rpool)
{
  input = VarStr(input.len, input.region_id, rpool->input_ptr);
  DecodeNode((uint8_t *) this, rpool);
}

size_t PromiseRoutine::NodeSize() const
{
  size_t s = util::Align(sizeof(PromiseRoutine), 8)
             + util::Align(capture_len, 8)
             + 8;
  if (next) {
    for (auto child: next->handlers) {
      s += child->NodeSize();
    }
  }
  return s;
}

uint8_t *PromiseRoutine::EncodeNode(uint8_t *p)
{
  memcpy(p, this, sizeof(PromiseRoutine));
  auto node = (PromiseRoutine *) p;
  p += util::Align(sizeof(PromiseRoutine), 8);
  node->capture_data = p;
  node->next = nullptr;
  node->pool = nullptr;

  memcpy(p, capture_data, capture_len);
  p += util::Align(capture_len, 8);
  size_t nr_children = 0;
  if (next) {
    nr_children = next->handlers.size();
  }
  memcpy(p, &nr_children, 8);
  p += 8;

  if (next) {
    for (auto child: next->handlers) {
      p = child->EncodeNode(p);
    }
  }
  return p;
}

uint8_t *PromiseRoutine::DecodeNode(uint8_t *p, PromiseRoutinePool *rpool)
{
  p += util::Align(sizeof(PromiseRoutine), 8);
  capture_data = p;
  p += util::Align(capture_len, 8);
  next = nullptr;
  pool = rpool;
  Ref();
  size_t nr_children = 0;
  memcpy(&nr_children, p, 8);
  p += 8;
  if (nr_children > 0) {
    next = new BasePromise();
    for (int i = 0; i < nr_children; i++) {
      auto child = (PromiseRoutine *) p;
      p = child->DecodeNode(p, rpool);
      next->handlers.push_back(child);
    }
  }
  return p;
}

void BasePromise::Complete(const VarStr &in)
{
  auto &configuration = util::Instance<NodeConfiguration>();
  for (auto routine: handlers) {
    routine->input = in;

    if (routine->node_id == -1) {
      routine->node_id = routine->placement(routine);
    }
    if (routine->node_id < 0) std::abort();

    if (routine->node_id == Instance<NodeConfiguration>().node_id()) {
      uint8_t *p = (uint8_t *) malloc(in.len);
      memcpy(p, in.data, in.len);
      routine->input = VarStr(in.len, in.region_id, p);

      go::GetSchedulerFromPool(1)->WakeUp(
          go::Make([routine]() { routine->callback(routine); }));

    } else {
      configuration.TransportPromiseRoutine(routine);
      routine->input.data = nullptr;
      routine->UnRefRecursively();
    }
  }
  UnRef();
}

std::atomic<BaseCombinerState *> BaseCombinerState::gStatesTable[BaseCombinerState::kStateTableSize];

}

// #define SAMPLE_PROMISE

#ifdef SAMPLE_PROMISE

static void TransportPromiseRoutine(PromiseRoutine *routine)
{
  size_t buffer_size = routine->TreeSize();
  printf("Tree size %ld\n", buffer_size);
  uint8_t *buffer = (uint8_t *) malloc(buffer_size);
  routine->EncodeTree(buffer);
  FILE *fp = fopen(std::to_string(routine->node_id).c_str(), "w");
  fwrite(&buffer_size, 8, 1, fp);
  fwrite(buffer, buffer_size, 1, fp);
  fclose(fp);
  free(buffer);
}

static void ParsePromiseRoutine()
{
  FILE *fp = fopen(std::to_string(Instance<NodeConfiguration>().node_id()).c_str(), "r");
  size_t buffer_size;
  (void) fread(&buffer_size, 8, 1, fp);
  PromiseRoutinePool *pool = PromiseRoutinePool::Create(buffer_size);
  (void) fread(pool->mem, buffer_size, 1, fp);
  fclose(fp);

  auto r = PromiseRoutine::CreateFromBufferedPool(pool);

  // TODO: testing only. add to pool 1.
  go::GetSchedulerFromPool(1)->WakeUp(
      go::Make([r]() { r->callback(r); }));
}

using namespace dolly;
using namespace sql;

int main(int argc, const char *argv[])
{
  go::InitThreadPool(1);

  if (argc <= 1) {
    auto _ = PromiseProc();

    auto left =
        _
        ->Then(argc, 1, [](const int &count, auto _) -> Optional<Tuple<int>> {
            int a;
            std::cin >> a;
            std::cout << "First, got A " << a << std::endl;
            return Tuple<int>(a);
          })

        ->Then(argc, 1, [](const int &count, Tuple<int> last_result) -> Optional<Tuple<int>> {
            printf("Last response is %d\n", last_result._<0>());
            return Tuple<int>(0);
          })

        ->Then(argc, 1, [](const int &count, auto _) -> Optional<Tuple<int>> {
            puts("End");
            return Tuple<int>(1);
          });

    auto right =
        _
        ->Then(argc, 2, [](const int &argc, auto _) -> Optional<Tuple<int>> {
            puts("This runs in parallel");
            return Tuple<int>(1);
          });
    Combine<Tuple<int>, Tuple<int>>(3, 1, left, right)
        ->Then(argc, 3, [](const int &argc, Tuple<Tuple<int>, Tuple<int>> input) -> Optional<VoidValue> {
            printf("sum = %d\n", input._<0>()._<0>() + input._<1>()._<0>());
            return nullopt;
          });
  } else {
    ParsePromiseRoutine();
  }

  go::WaitThreadPool();
  return 0;
}

#endif
