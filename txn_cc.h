// -*- mode: c++ -*-

#ifndef TXN_CC_H
#define TXN_CC_H

#include "slice.h"
#include "sqltypes.h"
#include "epoch.h"
#include "txn.h"
#include "contention_manager.h"
#include "piece_cc.h"

// shirley temp
#include <libpmem.h>

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

class PlaceholderParam {
  int nr;
 public:
  using TableType = void;
  PlaceholderParam(int nr = 1) : nr(nr) {}
  int size() const { return nr; }
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
  const Pair *begin() const { return pairs; }
  const Pair *end() const { return pairs + len; }

  void Add(int16_t node, uint16_t bitmap) {
    pairs[len++] = Pair(node, bitmap);
  }

  void MergeOrAdd(int16_t node, uint16_t bitmap) {
    for (int i = 0; i < len; i++) {
      auto [n, oldbitmap] = pairs[i];
      if (n == node) {
        pairs[i] = Pair(node, oldbitmap | bitmap);
        return;
      }
    }
    Add(node, bitmap);
  }

  NodeBitmap & operator+=(const NodeBitmap &rhs) {
    for (Pair e: rhs) {
      auto &[node, bitmap] = e;
      MergeOrAdd(node, bitmap);
    }
    return *this;
  }
};

template <typename T> class FutureValue;

template <>
class FutureValue<void> {
 protected:
  std::atomic_bool ready = false;
 public:
  FutureValue() {}
  FutureValue(const FutureValue<void> &rhs) : ready(rhs.ready.load()) {}
  const FutureValue<void> &operator=(const FutureValue<void> &rhs) {
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
        if (((BasePieceCollection::ExecutionRoutine *) routine)->Preempt()) {
          continue;
        }
      }
      _mm_pause();
    }
  }
};

template <typename T>
class FutureValue : public FutureValue<void> {
  T value;
 public:
  using ValueType = T;

  FutureValue() {}

  FutureValue(const FutureValue<T> &rhs) : value(rhs.value) {
    ready = rhs.ready.load();
  }

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
  using RowFuncPtr = void (*)(const Context&, IndexInfo *);

  RowFuncPtr rowfunc = nullptr;
  IndexInfo *row = nullptr;

  void ClearCallback() {
    row = nullptr;
    rowfunc = nullptr;
  }

  bool has_callback() const {
    return rowfunc;
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
  PieceCollection *root;
  State state;
 public:

  class TxnRow : public BaseTxnRow {
   public:
    using BaseTxnRow::BaseTxnRow;

    template <typename T> T Read(bool is_insert = false) {
      return ReadVarStr(is_insert)->template ToType<T>();
    }

    // shirley: since we want to allocate from inlined vhandle, 
    // we probably need do something similar to the previous WriteTryInline
    // to use vhandle->AllocFromInline, and EncodeFromPtrOrDefault
    template <typename T> bool Write(const T &o) {
      //shirley: probe size of version value
      // felis::probes::VersionValueSizeArray{(int)o.EncodeSize()}();
      if (!index_info) {
        printf("Write: index_info is null???\n");
        std::abort();
      }
      bool usePmem = ((index_info->last_version()) == sid);
      //shirley: probe transient vs persistent
      // probes::TransientPersistentCount{usePmem}();

      //shirley: if usePmem, try alloc from inline pmem and use o.EncodeToPtrOrDefault
      if (usePmem) {
        // felis::probes::NumReadWriteDramPmem{1,2,2}();
        // felis::probes::NumReadWriteDramPmem{1,1,2}();
        index_info->Prefetch_vhandle();
        auto val_sz = sizeof(VarStr) + o.EncodeSize();

        // shirley: assuming data region alloc will be successful.
        VarStr *val_dram = nullptr;
        bool result = false;
        if (!felis::Options::kDisableDramCache) {
          val_dram = o.EncodeToPtr(mem::GetDataRegion().Alloc(val_sz));
          val_dram->set_region_id(mem::ParallelPool::CurrentAffinity());
          result = WriteVarStr(val_dram);
        
          // shirley: update dram cache
          // shirley: do this before accessing vhandle, and prefetch vhandle
          index_info->dram_version->val = val_dram;//(VarStr*) mem::GetDataRegion().Alloc(val_sz);
          index_info->dram_version->ep_num = sid; // util::Instance<EpochManager>().current_epoch_nr();
        }

        // shirley: assume by now, prefetching vhandle has completed
        VHandle *vhandle = index_info->vhandle_ptr();
        // shirley: minor GC
        auto ptr2 = vhandle->GetInlinePtr(felis::SortedArrayVHandle::SidType2);
        // shirley: don't do minGC if is recovery bc dont have major GC list
        if (!felis::Options::kRecovery && ptr2){
          // shirley: with new design, external values always GC'd during major GC at end of epoch
          // vhandle->remove_majorGC_if_ext();
          // vhandle->FreePtr1(); 
          vhandle->Copy2To1();
        }

        auto sid2 = vhandle->GetInlineSid(felis::SortedArrayVHandle::SidType2);
        if ((felis::Options::kRecovery) && (sid2 >> 32 == sid >> 32)) {
          //i.e. before crash, we already wrote to this row.
          if (vhandle->is_inline_ptr(ptr2)) {
            // can directly copy to ptr2 if inlined bc of determinism
            if (!felis::Options::kDisableDramCache) {
              std::memcpy(ptr2, val_dram, val_sz);
            }
            else {
              o.EncodeToPtr(ptr2);
              result = WriteVarStr((VarStr *)ptr2);
            }
          }
          else {
            // should re-allocate if is external
            VarStr *val_ext = (VarStr *) (mem::GetExternalPmemPool().Alloc(true));
            if (!felis::Options::kDisableDramCache) {
              std::memcpy(val_ext, val_dram, val_sz);
            }
            else {
              o.EncodeToPtr(val_ext);
              val_ext->set_region_id(mem::ParallelPool::CurrentAffinity());
              result = WriteVarStr(val_ext);
            }
            vhandle->SetInlinePtr(felis::SortedArrayVHandle::SidType2,(uint8_t *)val_ext); 
          }
          vhandle->SetInlineSid(felis::SortedArrayVHandle::SidType2,sid); 
          vhandle->add_majorGC_if_ext();
          return result;
        }

        // alloc inline val and copy data
        VarStr *val = (VarStr *) (vhandle->AllocFromInline(val_sz, felis::SortedArrayVHandle::SidType2));
        if (!val){
          val = (VarStr *) (mem::GetExternalPmemPool().Alloc(true));
          // val = (VarStr *) (mem::GetPersistentPool().Alloc(val_sz));
        }
        if (!felis::Options::kDisableDramCache) {
          std::memcpy(val, val_dram, val_sz);
        }
        else {
          o.EncodeToPtr(val);
          val->set_region_id(mem::ParallelPool::CurrentAffinity());
          result = WriteVarStr(val);
        }

        // sid2 = sid;
        vhandle->SetInlineSid(felis::SortedArrayVHandle::SidType2,sid); 
        // ptr2 = val;
        vhandle->SetInlinePtr(felis::SortedArrayVHandle::SidType2,(uint8_t *)val); 
        // shirley: add to major GC if ptr1 inlined
        vhandle->add_majorGC_if_ext();

        // flush external varstr
        if (!(vhandle->is_inline_ptr((uint8_t *)val))) {
          for (int val_i = 0; val_i < val_sz; val_i += 64) {
            //shirley pmem shirley test
            _mm_clwb((char *)val + val_i);
          }
        }
        
        //shirley pmem shirley test: flush cache after last version write
        _mm_clwb((char *)vhandle);
        _mm_clwb((char *)vhandle + 64);
        _mm_clwb((char *)vhandle + 128);
        _mm_clwb((char *)vhandle + 192);
        //shirley: flush val in case it's external? need to check size, might be larger than 64 bytes
        
        // shirley temp: for ycsb all inlined, flush val that's within vhandle
        // shirley: commented out vhandle flush above except first bc flushing inlined val manually
        // pmem_flush(val, val_sz);

        return result;
      }
      else {
        // felis::probes::NumReadWriteDramPmem{1,0,2}();
        VarStr *val_transient = o.Encode(usePmem);
        bool result = WriteVarStr(val_transient);
        // shirley zen: comparing Zen, also write varstr to pmem and flush varstr
        if (felis::Options::kEnableZen) {
          size_t varlen = sizeof(VarStr) + o.EncodeSize() + 12; // + 12 bc Zen uses 16 bytes header, we use 4 in VarStr.
          VarStr *val_pmem = (VarStr *)mem::GetTransientPmemPool().Alloc(varlen);
          std::memcpy(val_pmem, val_transient, varlen - 12);
          *((char *)val_pmem + varlen - 12) = (uint32_t)sid; // simulate writing metadata in Zen to each pmem tuple.
          *((char *)val_pmem + varlen - 8) = (uint32_t)sid; // simulate writing metadata in Zen to each pmem tuple.
          *((char *)val_pmem + varlen - 4) = (uint32_t)sid; // simulate writing metadata in Zen to each pmem tuple.
          // shirley pmem shirley test
          for (int i = 0; i < varlen; i += 64) {
            _mm_clwb(((char *)val_pmem) + i);
          }
        }
        return result;
      }
    }

    // shirley: WriteTryInline is the same as Write
    // template <typename T> bool WriteTryInline(const T &o) {
    //   // shirley: removed. not used. should always use Write.
    // }

    //shirley: this should be used for writing initial version after row insert. Not used for database setup.
    template <typename T> bool WriteInitialInline(const T &o) {
      // felis::probes::NumReadWriteDramPmem{1,2,0}();
      //shirley: probe size of version value
      // felis::probes::VersionValueSizeArray{(int)o.EncodeSize()}();

      // shirley try: WRITING TO DRAM CACHE
      // auto temp_dram_version = (DramVersion*) mem::GetDataRegion().Alloc(sizeof(DramVersion));
      // temp_dram_version->val = o.EncodeToPtr(mem::GetDataRegion().Alloc(sizeof(VarStr) + o.EncodeSize()));
      // int curAffinity = mem::ParallelPool::CurrentAffinity();
      // ((VarStr*)(temp_dram_version->val))->set_region_id(curAffinity);
      // temp_dram_version->ep_num = sid; // curr_ep_nr;// util::Instance<EpochManager>().current_epoch_nr();
      // temp_dram_version->this_coreid = curAffinity; 
      // index_info->dram_version = temp_dram_version;
      // uint64_t curr_ep_nr = util::Instance<EpochManager>().current_epoch_nr();
      // util::Instance<GC_Dram>().AddRow(index_info, curr_ep_nr);
      // shirley try: end of writing to dram cache

      
      //shirley: initial version (after insert) should be inlined if possible.
      bool usePmem = true;
      VHandle *vhandle = index_info->vhandle_ptr();
      //shirley: probe transient vs persistent
      // probes::TransientPersistentCount{usePmem}();
      VarStr *val = o.EncodeToPtrOrDefault(vhandle->AllocFromInline(sizeof(VarStr) + o.EncodeSize()), usePmem);
      
      // // shirley: also init dram cache
      // auto temp_dram_version = (DramVersion*) mem::GetDataRegion().Alloc(sizeof(DramVersion));
      // temp_dram_version->val = (VarStr*) mem::GetDataRegion().Alloc(VarStr::NewSize(val->length()));
      // std::memcpy(temp_dram_version->val, val, VarStr::NewSize(val->length()));
      // int curAffinity = mem::ParallelPool::CurrentAffinity();
      // ((VarStr*)(temp_dram_version->val))->set_region_id(curAffinity);
      // uint64_t curr_ep_nr = util::Instance<EpochManager>().current_epoch_nr();
      // temp_dram_version->ep_num = sid; // curr_ep_nr;// util::Instance<EpochManager>().current_epoch_nr();
      // temp_dram_version->this_coreid = curAffinity; 
      // index_info->dram_version = temp_dram_version;
      // util::Instance<GC_Dram>().AddRow(index_info, curr_ep_nr);

      if (!val) {
        printf("WriteInitialInline val is null?\n");
        std::abort();
      }
      // vhandle -> sid1 = sid
      vhandle->SetInlineSid(felis::SortedArrayVHandle::SidType1,sid); 
      // vhandle -> ptr1 = val
      vhandle->SetInlinePtr(felis::SortedArrayVHandle::SidType1,(uint8_t *)val); 
      
      // flush external varstr
      if (!(vhandle->is_inline_ptr((uint8_t *)val))) {
        for (int val_i = 0; val_i < sizeof(VarStr) + o.EncodeSize(); val_i += 64) {
          //shirley pmem shirley test
          _mm_clwb((char *)val + val_i);
        }
      }
      
      //shirley pmem shirley test: flush cache after insert
      _mm_clwb((char *)vhandle);
      _mm_clwb((char *)vhandle + 64);
      _mm_clwb((char *)vhandle + 128);
      _mm_clwb((char *)vhandle + 192);
      //shirley: don't need flush val. first insert always inlined in miniheap (max varstr is 83 bytes?)

      // shirley temp: for ycsb all inlined, flush val that's within vhandle
      // shirley: commented out vhandle flush except first above bc flushing inlined val here
      // pmem_flush(val, sizeof(VarStr) + o.EncodeSize());

      //shirley: remove call to WriteVarStr, simply set vhandle->ptr1 to the result of o.EncodeToPtrOrDefault
      return true;
      // return WriteVarStr(val);
    }
  };

  class TxnHandle : public BaseTxnHandle {
   public:
    using BaseTxnHandle::BaseTxnHandle;
    TxnHandle(const BaseTxnHandle &rhs) : BaseTxnHandle(rhs) {}

    TxnRow operator()(IndexInfo *index_info) const { return TxnRow(sid, epoch_nr, index_info); }
  };

  TxnHandle index_handle() const { return TxnHandle(sid, epoch->id()); }

  struct TxnIndexOpContext : public BaseTxn::BaseTxnIndexOpContext {
   private:
    template <typename R>
    int _FromKeyParam(uint16_t bitmap, int bitshift, int shift, R param) {
      for (int i = bitshift; i < kMaxPackedKeys && i < bitshift + param.size(); i++) {
        if constexpr (!std::is_void<typename R::TableType>::value) {
          if (bitmap & (1 << i)) {
            auto view = param[i - bitshift].EncodeViewRoutine();
            key_len[shift] = view.length();
            key_data[shift] = view.data();
            relation_ids[shift] = R::kRelationId;
            slice_ids[shift] = param.EncodeToSliceId(i - bitshift);

            shift++;
          }
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

  PieceCollection *root_promise() override final { return root; }
  void ResetRoot() override final { root = new PieceCollection(); }

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
      int node, IndexInfo *row,
      typename InvokeHandle<TxnState, Types...>::RowFuncPtr rowfunc,
      Types... params) {
    auto &conf = util::Instance<NodeConfiguration>();
    auto aff = UpdateForKeyAffinity(node, row);
    InvokeHandle<TxnState, Types...> invoke_handle{rowfunc, row};

    if (aff != -1 && !EpochClient::g_enable_granola && !EpochClient::g_enable_pwv) {
      root->AttachRoutine(
          sql::MakeTuple(invoke_handle, MakeContext(params...)),
          node,
          [](const auto &t) {
            auto &[invoke_handle, ctx] = t;
            invoke_handle.InvokeWithContext(ctx);
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
    if constexpr (!std::is_void<typename KParam::TableType>::value) {
      auto &locator = util::Instance<SliceLocator<typename KParam::TableType>>();
      for (int i = 0; i < param.size(); i++) {
        auto node = util::Instance<NodeConfiguration>().node_id();
        auto slice_id = locator.Locate(param[i]);
        if (slice_id >= 0) node = Router::SliceToNodeId(slice_id);
        bitmap_per_node[node] |= 1 << (i + bitshift);
      }
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

  static constexpr uint64_t kIndexOpFlatten = std::numeric_limits<uint32_t>::max();
  uint64_t txn_indexop_affinity = std::numeric_limits<uint64_t>::max();

  template <typename IndexOp,
            typename OnCompleteParam,
            typename OnComplete,
            typename ...KParams>
  NodeBitmap TxnIndexOpWithNodeBitmap(NodeBitmap nodes_bitmap,
                                      OnCompleteParam *pp,
                                      KParams ...params) {
    auto current_node = util::Instance<NodeConfiguration>().node_id();
    for (auto &p: nodes_bitmap) {
      auto [node, bitmap] = p;
      auto op_ctx = TxnIndexOpContextEx<OnCompleteParam>(
          index_handle(), state, bitmap, params...);

      if constexpr(!std::is_void<OnCompleteParam>()) {
        op_ctx.set_extra(*pp);
      }

      if ((node != 0 && current_node != node)
          || (VHandleSyncService::g_lock_elision && txn_indexop_affinity != kIndexOpFlatten)) {
        root->AttachRoutine(
            op_ctx, node,
            [](auto &ctx) {
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
            },
            txn_indexop_affinity);
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
    using ResultType = LookupRowResult;
    LookupRowResult result;
    TxnIndexLookupOpImpl(const BaseTxnIndexOpContext &ctx, int idx) {
      result = BaseTxnIndexOpLookup(ctx, idx);
    }
  };
  struct TxnIndexInsertOpImpl {
    using ResultType = IndexInfo *;
    IndexInfo *result;
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
