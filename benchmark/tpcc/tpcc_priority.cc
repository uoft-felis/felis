#include "benchmark/tpcc/tpcc_priority.h"
#include "benchmark/tpcc/new_order.h"

namespace tpcc {

void GeneratePriorityTxn() {
  if (!NodeConfiguration::g_priority_txn)
    return;
  int txn_per_epoch = felis::PriorityTxnService::g_nr_priority_txn;
  for (auto i = 1; i < felis::EpochClient::g_max_epoch; ++i) {
    felis::PriorityTxn txn(&NewOrderTxn_Run);
    txn.epoch = i;
    txn.delay = 2200 * felis::PriorityTxnService::g_interval_priority_txn;
    util::Instance<felis::PriorityTxnService>().PushTxn(&txn);

    for (auto j = 2; j <= txn_per_epoch; ++j) {
      felis::PriorityTxn txn(&StockTxn_Run);
      txn.epoch = i;
      txn.delay = 2200 * felis::PriorityTxnService::g_interval_priority_txn * j;
      util::Instance<felis::PriorityTxnService>().PushTxn(&txn);
    }
  }
  logger->info("[Pri-init] pri txns pre-generated, {} per epoch", txn_per_epoch);
}

template <>
StockTxnInput ClientBase::GenerateTransactionInput<StockTxnInput>()
{
  StockTxnInput in;
  in.warehouse_id = PickWarehouse();
  in.nr_items = 20; // RandomNumber(1, StockTxnInput::kStockMaxItems);

  for (int i = 0; i < in.nr_items; i++) {
 again:
    auto id = GetItemId();
    // Check duplicates. Got this from NewOrder.
    for (int j = 0; j < i; j++)
      if (in.detail.item_id[j] == id) goto again;
    in.detail.item_id[i] = id;
    in.detail.stock_quantities[i] = RandomNumber(50, 100);
  }
  return in;
}

std::string format_sid(uint64_t sid)
{
  return "node_id " + std::to_string(sid & 0x000000FF) +
         ", epoch " + std::to_string(sid >> 32) +
         ", txn sequence " + std::to_string(sid >> 8 & 0xFFFFFF);
}

bool StockTxn_Run(felis::PriorityTxn *txn)
{
  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t diff = start_tsc - (txn->delay + felis::PriorityTxnService::g_tsc);
  felis::probes::PriInitQueueTime{diff / 2200, txn->epoch, txn->delay}();

  // generate txn input
  StockTxnInput txnInput = dynamic_cast<tpcc::Client*>
      (felis::EpochClient::g_workload_client)->GenerateTransactionInput<StockTxnInput>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < txnInput.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(txnInput.warehouse_id,
                                         txnInput.detail.item_id[i]));
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // init
  std::vector<felis::VHandle*> stock_rows;
  for (auto key : stock_keys) {
    felis::VHandle* row = nullptr;
    if (!(txn->InitRegisterUpdate<tpcc::Stock>(key, row))) {
      // debug(TRACE_PRIORITY "init register failed!");
      std::abort();
    }
    stock_rows.push_back(row);
  }
  uint64_t fail_tsc = start_tsc;
  int fail_cnt = 0;
  while (!txn->Init()) {
    fail_tsc = __rdtsc();
    ++fail_cnt;
  }
  uint64_t succ_tsc = __rdtsc();
  txn->measure_tsc = succ_tsc;
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  // debug(TRACE_PRIORITY "Priority txn {:p} (stock) - Init() succuess, sid {} - {}", (void *)txn, txn->serial_id(), format_sid(txn->serial_id()));
  felis::probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();


  struct Context {
    uint warehouse_id;
    uint nr_items;
    felis::PriorityTxn *txn;
    uint stock_quantities[StockTxnInput::kStockMaxItems];
    felis::VHandle* stock_rows[StockTxnInput::kStockMaxItems];
  };

  // issue promise
  int core_id = util::Instance<felis::PriorityTxnService>().GetFastestCore();
  // int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t cur_prog = util::Instance<felis::PriorityTxnService>().GetProgress(core_id) >> 8;
  uint64_t seq = (txn->serial_id() >> 8);
  uint64_t diff_to_cur_progress = (seq > cur_prog) ? (seq - cur_prog) : 0;
  felis::probes::Distance{diff_to_cur_progress, txn->serial_id()}();
  // distance: how many txn sids from the acquired sid to the core's current progress

  auto lambda =
      [](std::tuple<Context> capture) {
        auto [ctx] = capture;
        auto piece_id = ctx.txn->piece_count.fetch_sub(1);

        // record exec queue time
        auto queue_tsc = __rdtsc();
        auto diff = queue_tsc - ctx.txn->measure_tsc;
        felis::probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
        ctx.txn->measure_tsc = queue_tsc;

        for (int i = 0; i < ctx.nr_items; ++i) {
          auto stock = ctx.txn->Read<Stock::Value>(ctx.stock_rows[i]);
          stock.s_quantity += ctx.stock_quantities[i];
          ctx.txn->Write(ctx.stock_rows[i], stock);
          ClientBase::OnUpdateRow(ctx.stock_rows[i]);
        }

        // record exec time
        auto exec_tsc = __rdtsc();
        auto exec = exec_tsc - ctx.txn->measure_tsc;
        auto total = exec_tsc - (ctx.txn->delay + felis::PriorityTxnService::g_tsc);
        felis::probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
      };
  Context ctx{txnInput.warehouse_id,
              txnInput.nr_items,
              txn};
  memcpy(ctx.stock_quantities, txnInput.detail.stock_quantities, sizeof(uint) * ctx.nr_items);
  memcpy(ctx.stock_rows, &stock_rows[0], sizeof(felis::VHandle*) * ctx.nr_items);
  txn->IssuePromise(ctx, lambda);
  // debug(TRACE_PRIORITY "Priority txn {:p} (stock) - Issued lambda into PQ", (void *)txn);

  // record exec issue time
  uint64_t issue_tsc = __rdtsc();
  diff = issue_tsc - succ_tsc;
  txn->measure_tsc = issue_tsc;
  felis::probes::PriExecIssueTime{diff / 2200, txn->serial_id()}();

  return txn->Commit();
}

bool NewOrderTxn_Run(felis::PriorityTxn *txn)
{
  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t diff = start_tsc - (txn->delay + felis::PriorityTxnService::g_tsc);
  felis::probes::PriInitQueueTime{diff / 2200, txn->epoch, txn->delay}();

  // generate txn input
  NewOrderStruct input = dynamic_cast<tpcc::Client*>
      (felis::EpochClient::g_workload_client)->GenerateTransactionInput<NewOrderStruct>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < input.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(input.detail.supplier_warehouse_id[i],
                                         input.detail.item_id[i]));
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // register update
  std::vector<felis::VHandle*> stock_rows;
  for (auto key : stock_keys) {
    felis::VHandle* row = nullptr;
    if (!(txn->InitRegisterUpdate<tpcc::Stock>(key, row))) {
      // debug(TRACE_PRIORITY "init register failed!");
      std::abort();
    }
    stock_rows.push_back(row);
  }

  // register insert
  auto auto_inc_zone = input.warehouse_id * 10 +  input.district_id;
  auto oorder_id = dynamic_cast<tpcc::Client*>(felis::EpochClient::g_workload_client)->
                   relation(OOrder::kTable).AutoIncrement(auto_inc_zone);

  auto oorder_key = OOrder::Key::New(input.warehouse_id, input.district_id, oorder_id);
  auto neworder_key = NewOrder::Key::New(input.warehouse_id, input.district_id, oorder_id, input.customer_id);
  OrderLine::Key orderline_keys[input.kNewOrderMaxItems];
  for (int i = 0; i < input.nr_items; i++)
    orderline_keys[i] = OrderLine::Key::New(input.warehouse_id, input.district_id, oorder_id, i + 1);

  felis::BaseInsertKey *oorder_ikey, *neworder_ikey, *orderline_ikeys[input.kNewOrderMaxItems];
  txn->InitRegisterInsert<tpcc::OOrder>(oorder_key, oorder_ikey);
  txn->InitRegisterInsert<tpcc::NewOrder>(neworder_key, neworder_ikey);
  for (int i = 0; i < input.nr_items; i++)
    txn->InitRegisterInsert<tpcc::OrderLine>(orderline_keys[i], orderline_ikeys[i]);

  // init
  uint64_t fail_tsc = start_tsc;
  int fail_cnt = 0;
  while (!txn->Init()) {
    fail_tsc = __rdtsc();
    ++fail_cnt;
  }
  uint64_t succ_tsc = __rdtsc();
  txn->measure_tsc = succ_tsc;
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  // debug(TRACE_PRIORITY "Priority txn {:p} (neworder) - Init() succuess, sid {} - {}", (void *)txn, txn->serial_id(), format_sid(txn->serial_id()));
  felis::probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();


  struct Context {
    NewOrderStruct in;
    felis::PriorityTxn *txn;
    felis::BaseInsertKey *oorder_ikey;
    felis::BaseInsertKey *neworder_ikey;
    felis::BaseInsertKey *orderline_ikeys[NewOrderStruct::kNewOrderMaxItems];
    felis::VHandle* stock_rows[NewOrderStruct::kNewOrderMaxItems];
  };
  // issue promise
  int core_id = util::Instance<felis::PriorityTxnService>().GetFastestCore();
  // int core_id = go::Scheduler::CurrentThreadPoolId() - 1;
  uint64_t cur_prog = util::Instance<felis::PriorityTxnService>().GetProgress(core_id) >> 8;
  uint64_t seq = (txn->serial_id() >> 8);
  uint64_t diff_to_cur_progress = (seq > cur_prog) ? (seq - cur_prog) : 0;
  felis::probes::Distance{diff_to_cur_progress, txn->serial_id()}();
  // distance: how many txn sids from the acquired sid to the core's current progress

  auto lambda =
      [](std::tuple<Context> capture) {
        auto [ctx] = capture;
        auto piece_id = ctx.txn->piece_count.fetch_sub(1);

        // record exec queue time
        auto queue_tsc = __rdtsc();
        auto diff = queue_tsc - ctx.txn->measure_tsc;
        felis::probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
        ctx.txn->measure_tsc = queue_tsc;

        // update stock
        bool all_local = true;
        for (int i = 0; i < ctx.in.nr_items; ++i) {
          auto stock = ctx.txn->Read<Stock::Value>(ctx.stock_rows[i]);
          if (stock.s_quantity - ctx.in.detail.order_quantities[i] < 10) {
            stock.s_quantity += 91;
          }
          stock.s_quantity -= ctx.in.detail.order_quantities[i];
          stock.s_ytd += ctx.in.detail.order_quantities[i];
          if (ctx.in.detail.supplier_warehouse_id[i] != ctx.in.warehouse_id) {
            stock.s_remote_cnt++;
            all_local = false;
          }
          ctx.txn->Write(ctx.stock_rows[i], stock);
          ClientBase::OnUpdateRow(ctx.stock_rows[i]);
        }
        // update (inserted) oorder, neworder
        auto oorder_value = OOrder::Value::New(ctx.in.customer_id, 0, ctx.in.nr_items,
                                               all_local, ctx.in.ts_now);
        ctx.txn->Write(ctx.txn->InsertKeyToVHandle(ctx.oorder_ikey), oorder_value);
        ctx.txn->Write(ctx.txn->InsertKeyToVHandle(ctx.neworder_ikey), NewOrder::Value());
        // update (inserted) orderline
        for (int i = 0; i < ctx.in.nr_items; ++i) {
          auto item = util::Instance<RelationManager>().Get<Item>().
                      Search(Item::Key::New(ctx.in.detail.item_id[i]).EncodeFromRoutine());
          auto item_value = ctx.txn->Read<Item::Value>(item);
          auto amount = item_value.i_price * ctx.in.detail.order_quantities[i];
          auto ol_value = OrderLine::Value::New(ctx.in.detail.item_id[i], 0, amount,
                                                ctx.in.detail.supplier_warehouse_id[i],
                                                ctx.in.detail.order_quantities[i]);
          ctx.txn->Write(ctx.txn->InsertKeyToVHandle(ctx.orderline_ikeys[i]), ol_value);
        }

        // record exec time
        auto exec_tsc = __rdtsc();
        auto exec = exec_tsc - ctx.txn->measure_tsc;
        auto total = exec_tsc - (ctx.txn->delay + felis::PriorityTxnService::g_tsc);
        felis::probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
      };
  Context ctx {input, txn, oorder_ikey, neworder_ikey};
  memcpy(ctx.orderline_ikeys, orderline_ikeys, sizeof(felis::BaseInsertKey) * input.kNewOrderMaxItems);
  memcpy(ctx.stock_rows, &stock_rows[0], sizeof(felis::VHandle*) * input.nr_items);
  txn->IssuePromise(ctx, lambda);
  // debug(TRACE_PRIORITY "Priority txn {:p} (neworder) - Issued lambda into PQ", (void *)txn);

  // record exec issue time
  uint64_t issue_tsc = __rdtsc();
  diff = issue_tsc - succ_tsc;
  txn->measure_tsc = issue_tsc;
  felis::probes::PriExecIssueTime{diff / 2200, txn->serial_id()}();

  return txn->Commit();
}

}
