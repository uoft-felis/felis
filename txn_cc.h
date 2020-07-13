#ifndef TXN_CC_H
#define TXN_CC_H

#include "slice.h"
#include "sqltypes.h"
#include "epoch.h"
#include "txn.h"
#include "contention_manager.h"

namespace felis {

// C++ api layer.
// TODO: some of the BaseTxn should be here.

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
  Pair pairs[BaseTxn::BaseTxnIndexOpContext::kMaxPackedKeys];
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

template <typename T> class FutureValue;

template <>
class FutureValue<void> {
  std::atomic_bool ready = false;
 public:
  FutureValue() {}
  FutureValue(const FutureValue &rhs) : ready(rhs.ready.load()) {}
  const FutureValue<void> &operator=(const FutureValue &rhs) {
    ready = rhs.ready.load();
    return *this;
  }
  void Signal() { ready = true; }
  void Wait() {
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
  }
};

template <typename T>
class FutureValue : public FutureValue<void> {
  std::atomic_bool ready = false;
  T value;
 public:
  using ValueType = T;

  FutureValue() {}

  FutureValue(const FutureValue &rhs) : ready(rhs.ready.load()), value(rhs.value) {}

  void Signal(T v) {
    value = v;
    FutureValue<void>::Signal();
  }
  T &Wait() {
    FutureValue<void>::Wait();
    return value;
  }
};

template <typename TxnState> class Txn;

template <typename TxnState, typename ...Types>
struct InvokeHandle {
  using Context = typename Txn<TxnState>::template ContextType<Types...>;
  using RowFuncPtr = void (*)(const Context&, VHandle *);

  RowFuncPtr rowfunc = nullptr;
  VHandle *row = nullptr;

  void ClearCallback() {
    row = nullptr;
    rowfunc = nullptr;
  }

  bool has_callback() const {
    return row && rowfunc;
  }

  void InvokeWithContext(const Context& ctx) const {
    if (has_callback())
      rowfunc(ctx, row);
  }

  void Invoke(const typename Txn<TxnState>::State &state,
              const typename Txn<TxnState>::TxnHandle &index_handle,
              Types... args) const {
    if (has_callback())
      InvokeWithContext(Context(state, index_handle, args...));
  }
};

template <typename TxnState>
class Txn : public BaseTxn {
 public:
  typedef GenericEpochObject<TxnState> State;

 protected:
  Promise<DummyValue> *root;
  State state;
 public:

  class TxnRow : public BaseTxnRow {
   public:
    using BaseTxnRow::BaseTxnRow;

    template <typename T> T Read() {
      return ReadVarStr()->template ToType<T>();
    }
    template <typename T> bool Write(const T &o) {
      return WriteVarStr(o.Encode());
    }

    template <typename T> bool WriteTryInline(const T &o) {
      return WriteVarStr(o.EncodeFromPtrOrDefault(vhandle->AllocFromInline(o.EncodeSize())));
    }
  };

  class TxnHandle : public BaseTxnHandle {
   public:
    using BaseTxnHandle::BaseTxnHandle;
    TxnHandle(const BaseTxnHandle &rhs) : BaseTxnHandle(rhs) {}

    TxnRow operator()(VHandle *vhandle) const { return TxnRow(sid, epoch_nr, vhandle); }
  };

  TxnHandle index_handle() const { return TxnHandle(sid, epoch->id()); }

  struct TxnIndexOpContext : public BaseTxn::BaseTxnIndexOpContext {
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
    template <typename ...T>
    TxnIndexOpContext(BaseTxnHandle handle, EpochObject state, uint16_t bitmap, T ...params) {
      this->handle = handle;
      this->state = state;
      this->keys_bitmap = this->slices_bitmap = this->rels_bitmap = bitmap;

      _FromKeyParam(bitmap, 0, 0, params...);
    }

    TxnIndexOpContext() {}
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

  Txn(uint64_t serial_id) : BaseTxn(serial_id) {}

  BasePromise *root_promise() override final { return root; }
  void ResetRoot() override final { root = new Promise<DummyValue>(); }

  void PrepareState() override {
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

  template <typename ...Types>
  InvokeHandle<TxnState, Types...> UpdateForKey(
      int node, VHandle *row,
      typename InvokeHandle<TxnState, Types...>::RowFuncPtr rowfunc,
      Types... params) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto aff = UpdateForKeyAffinity(node, row);
    InvokeHandle<TxnState, Types...> invoke_handle{rowfunc, row};

    if (aff != -1) {
      root->Then(
          sql::MakeTuple(invoke_handle, MakeContext(params...)),
          node,
          [](const auto &t, auto _) -> Optional<VoidValue> {
            auto &[invoke_handle, ctx] = t;
            invoke_handle.InvokeWithContext(ctx);
            return nullopt;
          },
          aff);
      invoke_handle.ClearCallback();
    }
    return invoke_handle;
  }

 private:
  template <typename Router, typename KParam, typename ...KParams>
  void KeyParamsToBitmap(uint16_t bitmap_per_node[],
                         int bitshift, KParam param, KParams ...rest) {
    auto &locator = util::Instance<SliceLocator<typename KParam::TableType>>();
    for (int i = 0; i < param.size(); i++) {
      auto node = util::Instance<NodeConfiguration>().node_id();
      auto slice_id = locator.Locate(param[i]);
      if (slice_id >= 0) node = Router::SliceToNodeId(slice_id);
      bitmap_per_node[node] |= 1 << (i + bitshift);
    }
    KeyParamsToBitmap<Router>(bitmap_per_node, bitshift + param.size(), rest...);
  }
  template <typename Router>
  void KeyParamsToBitmap(uint16_t bitmap_per_node[], int bitshift) {}
 public:
  template <typename Router, typename ...KParams>
  NodeBitmap GenerateNodeBitmap(KParams ...params) {
    auto &conf = util::Instance<NodeConfiguration>();
    uint16_t bitmap_per_node[conf.nr_nodes() + 1];
    NodeBitmap nodes_bitmap;
    std::fill(bitmap_per_node, bitmap_per_node + conf.nr_nodes() + 1, 0);
    KeyParamsToBitmap<Router>(bitmap_per_node, 0, params...);
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
  NodeBitmap TxnIndexOpWithNodeBitmap(NodeBitmap nodes_bitmap,
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
        root->Then(
            op_ctx, node,
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

        completion.handle = TxnHandle(op_ctx.handle);
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
            typename Router,
            typename OnCompleteParam,
            typename OnComplete,
            typename ...KParams>
  NodeBitmap TxnIndexOp(OnCompleteParam *pp,
                        KParams ...params) {
    return TxnIndexOpWithNodeBitmap<IndexOp, OnCompleteParam, OnComplete, KParams...>(
        GenerateNodeBitmap<Router>(params...),
        pp,
        params...);
  }

 public:
  struct TxnIndexLookupOpImpl {
    LookupRowResult result;
    TxnIndexLookupOpImpl(const BaseTxnIndexOpContext &ctx, int idx) {
      result = BaseTxnIndexOpLookup(ctx, idx);
    }
  };
  struct TxnIndexInsertOpImpl {
    VHandle *result;
    TxnIndexInsertOpImpl(const BaseTxnIndexOpContext &ctx, int idx) {
      result = BaseTxnIndexOpInsert(ctx, idx);
    }
  };
  template <typename Router,
            typename Completion,
            typename CompletionParam = void,
            typename ...KParams>
  NodeBitmap TxnIndexLookup(CompletionParam *pp,
                            KParams ...params) {
    return TxnIndexOp<TxnIndexLookupOpImpl,
                      Router,
                      CompletionParam,
                      Completion,
                      KParams...>(pp, params...);
  }

  template <typename Router,
            typename Completion,
            typename CompletionParam = void,
            typename ...KParams>
  NodeBitmap TxnIndexInsert(CompletionParam *pp,
                            KParams ...params) {
    return TxnIndexOp<TxnIndexInsertOpImpl,
                      Router,
                      CompletionParam,
                      Completion,
                      KParams...>(pp, params...);
  }
};

template <typename TxnState>
class TxnStateCompletion {
 protected:
  friend class Txn<TxnState>;
  typename Txn<TxnState>::TxnHandle handle;
  GenericEpochObject<TxnState> state;
};

}

#endif
