#ifndef TXN_H
#define TXN_H

#include <cstdlib>
#include <cstdint>
#include <array>
#include "epoch.h"
#include "util.h"
#include "sqltypes.h"
#include "promise.h"
#include "slice.h"
#include "vhandle_batchappender.h"
#include "opts.h"

namespace felis {

class VHandle;
class Relation;
class EpochClient;

class BaseTxn {
 protected:
  friend class EpochClient;

  Epoch *epoch;
  uint64_t sid;

  PromiseProc proc;

  using BrkType = std::array<mem::Brk *, NodeConfiguration::kMaxNrThreads / mem::kNrCorePerNode>;
  static BrkType g_brk;
  static int g_cur_numa_node;

 public:
  BaseTxn(uint64_t serial_id)
      : epoch(nullptr), sid(serial_id) {}

  static void *operator new(size_t nr_bytes) { return g_brk[g_cur_numa_node]->Alloc(nr_bytes); }
  static void operator delete(void *ptr) {}
  static void InitBrk(long nr_epochs);

  virtual void PrepareState() {}

  virtual ~BaseTxn() {}
  virtual void Prepare() = 0;
  virtual void PrepareInsert() = 0;
  virtual void Run() = 0;

  void RunAndAssignSchedulingKey() {
    Run();
    root_promise()->AssignSchedulingKey(serial_id());
  }

  Promise<DummyValue> *root_promise() {
    return proc.promise();
  }

  void ResetRoot() {
    proc.Reset();
  }

  uint64_t serial_id() const { return sid; }
  uint64_t epoch_nr() const { return sid >> 32; }

  template <typename Api>
  class TxnApi {
   protected:
    uint64_t sid;
    uint64_t epoch_nr;
    Api *api;
   public:
    TxnApi(uint64_t sid, uint64_t epoch_nr, Api *api)
        : sid(sid), epoch_nr(epoch_nr), api(api) {}
  };

  class TxnVHandle : public TxnApi<VHandle> {
   public:
    using TxnApi<VHandle>::TxnApi;
    void AppendNewVersion();

    VarStr *ReadVarStr();
    template <typename T> T Read() {
      return ReadVarStr()->ToType<T>();
    }

    bool WriteVarStr(VarStr *obj);
    template <typename T> bool Write(const T &o) {
      return WriteVarStr(o.Encode());
    }
    bool Delete() {
      return WriteVarStr(nullptr);
    }
  };

  class TxnHandle {
    uint64_t sid;
    uint64_t epoch_nr;
   public:
    TxnHandle(uint64_t sid, uint64_t epoch_nr) : sid(sid), epoch_nr(epoch_nr) {}
    TxnHandle() {}

    TxnVHandle operator()(VHandle *vhandle) const { return TxnVHandle(sid, epoch_nr, vhandle); }

    uint64_t serial_id() const { return sid; }
  };

  TxnHandle index_handle() { return TxnHandle{sid, epoch->id()}; }

  struct TxnIndexOpContext {
    static constexpr size_t kMaxPackedKeys = 15;
    TxnHandle handle;
    EpochObject state;

    // We can batch a lot of keys in the same context. We also should mark if
    // some keys are not used at all. Therefore, we need a bitmap.
    uint16_t keys_bitmap;
    uint16_t slices_bitmap;
    uint16_t rels_bitmap;

    uint16_t key_len[kMaxPackedKeys];
    const uint8_t *key_data[kMaxPackedKeys];
    int16_t slice_ids[kMaxPackedKeys];
    int16_t relation_ids[kMaxPackedKeys];

    template <typename Func>
    static void ForEachWithBitmap(uint16_t bitmap, Func f) {
      for (int i = 0, j = 0; i < kMaxPackedKeys; i++) {
        const uint16_t mask = (1 << i);
        if (bitmap & mask) {
          f(j, i);
          j++;
        }
      }
    }
   private:
    template <typename R>
    int _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param) {
      auto rel_id = R::kRelationId;
      for (int i = bitshift; i < kMaxPackedKeys && i < bitshift + param.size(); i++) {
        if (bitmap & (1 << i)) {
          auto varstr = param[i - bitshift].EncodeFromRoutine();
          key_len[shift] = varstr->len;
          key_data[shift] = varstr->data;
          relation_ids[shift] = rel_id;
          slice_ids[shift] = param.EncodeToSliceId(i - bitshift);

          shift++;
        }
      }
      return shift;
    }
    template <typename R, typename ...T>
    void _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param, T ...rest) {
      shift = _FromKeyParam(bitmap, bitshift, shift, param);
      _FromKeyParam(bitmap, bitshift + param.size(), shift, rest...);
    }
   public:

    // We don't need to worry about padding because TxnHandle is perfectly padded.
    // We also need to send three bitmaps.
    static constexpr size_t kHeaderSize =
        sizeof(TxnHandle) + sizeof(EpochObject)
        + sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint16_t);

    TxnIndexOpContext(TxnHandle handle, EpochObject state,
                      uint16_t keys_bitmap, VarStr **keys,
                      uint16_t slices_bitmap, int16_t *slice_ids,
                      uint16_t rels_bitmap, int16_t *rels);
    template <typename ...T>
    TxnIndexOpContext(TxnHandle handle, EpochObject state, uint16_t bitmap, T ...params)
        : handle(handle), state(state),
          keys_bitmap(bitmap), slices_bitmap(bitmap), rels_bitmap(bitmap) {
      _FromKeyParam(bitmap, 0, 0, params...);
    }

    TxnIndexOpContext() {}

    size_t EncodeSize() const;
    uint8_t *EncodeTo(uint8_t *buf) const;
    const uint8_t *DecodeFrom(const uint8_t *buf);
  };

  template <typename Extra>
  struct TxnIndexOpContextEx : public TxnIndexOpContext, public Extra {
    using TxnIndexOpContext::TxnIndexOpContext;

    void set_extra(const Extra &rhs) {
      (Extra &)(*this) = rhs;
    }

    size_t EncodeSize() const {
      return TxnIndexOpContext::EncodeSize() + Extra::EncodeSize();
    }
    uint8_t *EncodeTo(uint8_t *buf) const {
      return Extra::EncodeTo(TxnIndexOpContext::EncodeTo(buf));
    }
    const uint8_t *DecodeFrom(const uint8_t *buf) {
      return Extra::DecodeFrom(TxnIndexOpContext::DecodeFrom(buf));
    }
  };

  template <>
  struct TxnIndexOpContextEx<void> : public TxnIndexOpContext {
    using TxnIndexOpContext::TxnIndexOpContext;
  };

  static constexpr size_t kMaxRangeScanKeys = 32;
  using LookupRowResult = std::array<VHandle *, kMaxRangeScanKeys>;

 protected:
  struct TxnIndexLookupOpImpl {
    TxnIndexLookupOpImpl(const TxnIndexOpContext &ctx, int idx);
    LookupRowResult result;
  };
  struct TxnIndexInsertOpImpl {
    TxnIndexInsertOpImpl(const TxnIndexOpContext &ctx, int idx);
    VHandle *result;
  };
};

template <typename Table>
class KeyParam {
 public:
  static constexpr int kRelationId = static_cast<int>(Table::kTable);
  using TableType = Table;
 protected:
  const typename Table::Key *start;
  int len;
 public:
  KeyParam(const typename Table::Key &k)
      : start(&k), len(1) {}
  KeyParam(const typename Table::Key *start, int len)
      : start(start), len(len) {}

  int EncodeToSliceId(int idx) {
    return util::Instance<SliceLocator<TableType>>().Locate(start[idx]);
  }

  int size() const { return len; }
  const typename Table::Key &operator[](int idx) { return start[idx]; }
};

template <typename Table>
class RangeParam {
 public:
  static constexpr int kRelationId = static_cast<int>(Table::kTable);
  using TableType = Table;
 private:
  const typename Table::Key *start;
  const typename Table::Key *end;
 public:
  RangeParam(const typename Table::Key &start, const typename Table::Key &end)
      : start(&start), end(&end) {}

  int EncodeToSliceId(int idx) { return -1 - idx; }
  int size() const { return 2; }
  const typename Table::Key &operator[](int idx) {
    if (idx == 0) return *start;
    else if (idx == 1) return *end;
    std::abort();
  }
};

class NodeBitmap {
 public:
  using Pair = std::tuple<int16_t, uint16_t>;
 private:
  uint8_t len;
  Pair pairs[BaseTxn::TxnIndexOpContext::kMaxPackedKeys];
 public:
  NodeBitmap() : len(0) {}
  NodeBitmap(const NodeBitmap &rhs) : len(rhs.len) {
    std::copy(rhs.pairs, rhs.pairs + len, pairs);
  }

  uint8_t size() const { return len; }
  Pair *begin() { return pairs; }
  Pair *end() { return pairs + len; }

  void Add(int16_t node, uint16_t bitmap) {
    pairs[len++] = Pair(node, bitmap);
  }
};

template <typename T, size_t ClosureSize = 24>
class FutureValue {
 public:
  std::atomic_bool ready = false;
  T value;
  void (*fp)(const void *, const BaseTxn::TxnHandle &, void *) = nullptr;
  uint8_t closure_data[ClosureSize];
 public:
  using ValueType = T;
  void Signal(T v) { value = v; ready = true; }
  T &Wait() {
    long wait_cnt = 0;
    while (!ready) {
      wait_cnt++;
      if ((wait_cnt & 0x0FFFF) == 0) {
        auto routine = go::Scheduler::Current()->current_routine();
        if (((BasePromise::ExecutionRoutine *) routine)->Preempt()) {
          continue;
        }
      }
      _mm_pause();
    }
    return value;
  }

  template <typename G>
  void Attach(void (*func)(const void *, const BaseTxn::TxnHandle &, void *), G closure) {
    static_assert(sizeof(G) <= ClosureSize);
    __builtin_memcpy(closure_data, &closure, sizeof(G));
    fp = func;
  }

  bool has_callback() const { return fp != nullptr; }
  void Invoke(const void *state, const BaseTxn::TxnHandle &handle) {
    if (fp) fp(state, handle, closure_data);
  }
};

template <typename TxnState>
class Txn : public BaseTxn {
 public:
  typedef GenericEpochObject<TxnState> State;

 protected:
  State state;
 public:
  Txn(uint64_t serial_id) : BaseTxn(serial_id) {}

  void PrepareState() final override {
    epoch = util::Instance<EpochManager>().current_epoch();
    state = epoch->AllocateEpochObjectOnCurrentNode<TxnState>();
    // printf("state epoch %lu\n", state.nr());
  }

  template <typename ...Types> using ContextType = sql::Tuple<State, TxnHandle, Types...>;

  template <typename ...Types>
  ContextType<Types...> MakeContext(Types... params) {
    return ContextType<Types...>(state, index_handle(), params...);
  }

  template <typename Func, typename ...Types>
  std::tuple<ContextType<Types...>, int, Func>
  TxnProc(int node, Func func, Types... params) {
    return std::make_tuple(
        MakeContext(params...),
        node,
        func);
  }

  template <typename FutureType, typename RowFunc, typename ...Types>
  FutureType *UpdateForKey(FutureType *future,
                           int placement,
                           VHandle *row, RowFunc rowfunc, Types... params) {
    using RowFuncPtr = typename FutureType::ValueType (*)(const ContextType<Types...>&, VHandle *);
    auto w = row->nr_versions() - row->nr_updated();
    new (future) FutureType;

    if (Options::kVHandleBatchAppend && Options::kVHandleParallel) {
      auto &appender = util::Instance<BatchAppender>();
      auto total_scale = appender.contention_weight_end() - appender.contention_weight_begin();
      if (total_scale == 0
          || row->contention_weight() < appender.contention_weight_begin()
          || w <= EpochClient::g_vhandle_parallel_threshold)
        goto nosplit;

      static thread_local util::XORRandom64 local_rand;
      auto lower = row->contention_weight() - appender.contention_weight_begin();

      auto upper = lower + w;
      auto aff = lower * NodeConfiguration::g_nr_threads / total_scale;
      auto upper_aff = (upper - 1) * NodeConfiguration::g_nr_threads / total_scale;
      if (aff != upper_aff) {
        if (aff / mem::kNrCorePerNode != upper_aff / mem::kNrCorePerNode) {
          // They belong to different sockets
          auto numa_node = (lower + upper - 1) * NodeConfiguration::g_nr_threads
                           / 2 / total_scale / mem::kNrCorePerNode;
          if (aff / mem::kNrCorePerNode != numa_node) {
            lower = numa_node * mem::kNrCorePerNode * total_scale / NodeConfiguration::g_nr_threads;
          }
          if (upper_aff / mem::kNrCorePerNode != numa_node) {
            upper = ((numa_node + 1) * mem::kNrCorePerNode) * total_scale / NodeConfiguration::g_nr_threads;
          }
        }
        aff = local_rand.NextRange(lower, upper) * NodeConfiguration::g_nr_threads / total_scale;
      }

      root_promise()->Then(
          sql::MakeTuple(future, (RowFuncPtr) rowfunc, row, MakeContext(params...)),
          placement,
          [](const auto &t, auto _) -> Optional<VoidValue> {
            auto &[future, rowfunc, row, ctx] = t;
            future->Signal(rowfunc(ctx, row));
            return nullopt;
          },
          aff);
      return future;
    }

 nosplit:
      future->Attach(
          [](const void *state, const TxnHandle &index_handle, void *p) {
            auto *pclosure = (sql::Tuple<FutureType *, RowFuncPtr, VHandle *, Types...> *) p;
            auto future = pclosure->template _<0>();
            auto rowfunc = pclosure->template _<1>();
            auto row = pclosure->template _<2>();
            if constexpr(sizeof...(Types) > 0) {
              sql::TupleField<Types...> *pparams = pclosure;
              auto ctx = ContextType<Types...>(*(const State *) state, index_handle, *pparams);
              future->Signal(rowfunc(ctx, row));
            } else {
              future->Signal(rowfunc(ContextType<Types...>(*(State *) state, index_handle), row));
            }
          },
          sql::MakeTuple(future, (RowFuncPtr) rowfunc, row, params...));

    return future;
  }

#if 0
  template <typename RowFunc, typename LastFunc, typename ...Types>
  void TxnHotKeys(int node, VHandle** hot_begin, VHandle **hot_end,
                  RowFunc rowfunc, LastFunc lastfunc, Types... params) {
    using RowFuncPtr = void (*)(const ContextType<Types...> &, VHandle **);
    using LastFuncPtr = void (*)(const ContextType<Types...> &);

    uint64_t splitted_bitmap = 0;
    LastFuncPtr lastfunc_ptr = (LastFuncPtr) lastfunc;

    if (!Options::kVHandleBatchAppend || !Options::kVHandleParallel) {
      goto main_piece;
    } else {
      auto &appender = util::Instance<BatchAppender>();
      auto total_scale = appender.contention_weight_end() - appender.contention_weight_begin();

      abort_if(hot_end - hot_begin > 63, "TxnHotKeys does not support > 63 keys");

      if (total_scale == 0)
        goto main_piece;

      for (auto p = hot_begin; p != hot_end; p++) {
        auto w = (*p)->nr_versions() - (*p)->nr_updated();
        if ((*p)->contention_weight() < appender.contention_weight_begin()
            || w <= Options::kVHandleParallel.ToInt() * EpochClient::g_txn_per_epoch / 1000)
          continue;

        splitted_bitmap |= 1ULL << (p - hot_begin);
        auto routine = root_promise()->AttachRoutine(
            sql::MakeTuple(p, (RowFuncPtr) rowfunc, MakeContext(params...)),
            [](const sql::Tuple<VHandle **, RowFuncPtr, ContextType<Types...>> &ctx, DummyValue _)
            -> Optional<VoidValue> {
              auto &[p, rowfunc, real_ctx] = ctx;
              rowfunc(real_ctx, p);
              return nullopt;
            });
        static thread_local util::XORRandom64 local_rand;
        auto lower = (*p)->contention_weight() - appender.contention_weight_begin();
        auto upper = lower + w;
        auto aff = lower * NodeConfiguration::g_nr_threads / total_scale;
        auto upper_aff = (upper - 1) * NodeConfiguration::g_nr_threads / total_scale;
        if (aff != upper_aff) {
          if (aff / mem::kNrCorePerNode != upper_aff / mem::kNrCorePerNode) {
            // They belong to different sockets
            auto numa_node = (lower + upper - 1) * NodeConfiguration::g_nr_threads
                             / 2 / total_scale / mem::kNrCorePerNode;
            if (aff / mem::kNrCorePerNode != numa_node) {
              lower = numa_node * mem::kNrCorePerNode * total_scale / NodeConfiguration::g_nr_threads;
            }
            if (upper_aff / mem::kNrCorePerNode != numa_node) {
              upper = ((numa_node + 1) * mem::kNrCorePerNode) * total_scale / NodeConfiguration::g_nr_threads;
            }
          }
          aff = local_rand.NextRange(lower, upper) * NodeConfiguration::g_nr_threads / total_scale;
        }
        routine->affinity = aff;
        routine->level = 0;
        routine->node_id = node;
        routine->next = nullptr;

        // put routine at the beginning of execution turns out isn't a good idea
        // routine->sched_key = serial_id() & 0x00FFFFFFFF;
        routine->sched_key = serial_id();
      }
    }

    // There is nothing in the main_piece. Both lastfunc and the main piece have
    // to be none.
    if (lastfunc_ptr == nullptr && __builtin_popcount(splitted_bitmap) == hot_end - hot_begin)
      return;

 main_piece:

    proc
        | std::tuple(
            sql::MakeTuple(hot_begin, hot_end, (RowFuncPtr) rowfunc, lastfunc_ptr, splitted_bitmap, MakeContext(params...)),
            node,
            [](const sql::Tuple<VHandle **, VHandle **, RowFuncPtr, LastFuncPtr, uint64_t, ContextType<Types...>> &ctx, DummyValue _)
            -> Optional<VoidValue> {
              auto &[hot_begin, hot_end, rowfunc, lastfunc, splitted_bitmap, real_ctx] = ctx;
              for (auto p = hot_begin; p != hot_end; p++) {
                if (splitted_bitmap & (1ULL << (p - hot_begin))) continue;
                rowfunc(real_ctx, p);
              }
              if (lastfunc)
                lastfunc(real_ctx);
              return nullopt;
            });
  }
#endif

 private:
  template <typename KParam, typename ...KParams>
  void KeyParamsToBitmap(SliceRoute router, uint16_t bitmap_per_node[],
                         int bitshift, KParam param, KParams ...rest) {
    auto &locator = util::Instance<SliceLocator<typename KParam::TableType>>();
    for (int i = 0; i < param.size(); i++) {
      auto node = util::Instance<NodeConfiguration>().node_id();
      auto slice_id = locator.Locate(param[i]);
      if (slice_id >= 0) node = router(slice_id);
      bitmap_per_node[node] |= 1 << (i + bitshift);
    }
    KeyParamsToBitmap(router, bitmap_per_node, bitshift + param.size(), rest...);
  }
  void KeyParamsToBitmap(SliceRoute router, uint16_t bitmap_per_node[], int bitshift) {}
 public:
  template <typename ...KParams>
  NodeBitmap GenerateNodeBitmap(SliceRoute router, KParams ...params) {
    auto &conf = util::Instance<NodeConfiguration>();
    uint16_t bitmap_per_node[conf.nr_nodes() + 1];
    NodeBitmap nodes_bitmap;
    std::fill(bitmap_per_node, bitmap_per_node + conf.nr_nodes() + 1, 0);
    KeyParamsToBitmap(router, bitmap_per_node, 0, params...);
    for (int node = 1; node <= conf.nr_nodes(); node++) {
      if (bitmap_per_node[node] == 0) continue;
      nodes_bitmap.Add(node, bitmap_per_node[node]);
    }
    return nodes_bitmap;
  }

  template <typename IndexOp,
            typename OnCompleteParam,
            typename OnComplete,
            typename ...KParams>
  NodeBitmap TxnIndexOp(NodeBitmap nodes_bitmap,
                        OnCompleteParam *pp,
                        KParams ...params) {
    for (auto &p: nodes_bitmap) {
      auto [node, bitmap] = p;
      auto op_ctx = TxnIndexOpContextEx<OnCompleteParam>(
          index_handle(), state, bitmap, params...);

      if constexpr(!std::is_void<OnCompleteParam>()) {
          op_ctx.set_extra(*pp);
        }

      if (!EpochClient::g_enable_granola) {
        proc
            | std::make_tuple(
                op_ctx,
                node,
                [](auto &ctx, auto _) -> Optional<VoidValue> {
                  auto completion = OnComplete();
                  if constexpr (!std::is_void<OnCompleteParam>()) {
                    completion.args = (OnCompleteParam) ctx;
                  }

                  completion.handle = ctx.handle;
                  completion.state = State(ctx.state);

                  TxnIndexOpContext::ForEachWithBitmap(
                      ctx.keys_bitmap,
                      [&ctx, &completion](int j, int i) {
                        auto op = IndexOp(ctx, j);
                        completion(i, op.result);
                      });
                  return nullopt;
                });
      } else {
        auto completion = OnComplete();
        if constexpr (!std::is_void<OnCompleteParam>()) {
          completion.args = (OnCompleteParam) op_ctx;
        }

        completion.handle = op_ctx.handle;
        completion.state = State(op_ctx.state);

        TxnIndexOpContext::ForEachWithBitmap(
            op_ctx.keys_bitmap,
            [&op_ctx, &completion](int j, int i) {
              auto op = IndexOp(op_ctx, j);
              completion(i, op.result);
            });
      }
    }
    return nodes_bitmap;
  }

  template <typename IndexOp,
            typename OnCompleteParam,
            typename OnComplete,
            typename ...KParams>
  NodeBitmap TxnIndexOp(SliceRoute router,
                        OnCompleteParam *pp,
                        KParams ...params) {
    return TxnIndexOp<IndexOp, OnCompleteParam, OnComplete, KParams...>(
        GenerateNodeBitmap(router, params...),
        pp,
        params...);
  }

 public:
  template <typename Completion,
            typename CompletionParam = void,
            typename Route,
            typename ...KParams>
  NodeBitmap TxnIndexLookup(Route r,
                            CompletionParam *pp,
                            KParams ...params) {
    return TxnIndexOp<BaseTxn::TxnIndexLookupOpImpl,
                      CompletionParam,
                      Completion,
                      KParams...>(r, pp, params...);
  }

  template <typename Completion,
            typename CompletionParam = void,
            typename Route,
            typename ...KParams>
  NodeBitmap TxnIndexInsert(Route r,
                            CompletionParam *pp,
                            KParams ...params) {
    return TxnIndexOp<BaseTxn::TxnIndexInsertOpImpl,
                      CompletionParam,
                      Completion,
                      KParams...>(r, pp, params...);
  }
};

template <typename TxnState>
class TxnStateCompletion {
 protected:
  friend class Txn<TxnState>;
  BaseTxn::TxnHandle handle;
  GenericEpochObject<TxnState> state;
};

}

#endif /* TXN_H */
