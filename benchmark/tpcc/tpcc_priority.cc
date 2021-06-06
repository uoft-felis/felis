#include "benchmark/tpcc/tpcc_priority.h"
#include "benchmark/tpcc/new_order.h"

namespace tpcc {

using namespace felis;

void GeneratePriorityTxn() {
  if (!NodeConfiguration::g_priority_txn)
    return;
  int txn_per_epoch = PriorityTxnService::g_nr_priority_txn;
  int stock_pct = 20;
  util::FastRandom r(__rdtsc());
  for (auto i = 1; i < EpochClient::g_max_epoch; ++i) {
    for (auto j = 1; j <= txn_per_epoch; ++j) {
      PriorityTxn txn;
      int pct = r.next_u32() % 100 + 1;
      if (pct < stock_pct)
        txn.SetCallback(&StockTxn_Run);
      else
        txn.SetCallback(&NewOrderDeliveryTxn_Run);
      txn.epoch = i;
      auto interval = PriorityTxnService::g_interval_priority_txn;
      txn.delay = static_cast<uint64_t>(static_cast<double>(interval * j) * 2.2);
      util::Instance<PriorityTxnService>().PushTxn(&txn);
    }
  }
  logger->info("[Pri-init] pri txns pre-generated, {} per epoch", txn_per_epoch);
}

template <>
StockTxnInput ClientBase::GenerateTransactionInput<StockTxnInput>()
{
  StockTxnInput in;
  in.warehouse_id = PickWarehouse();
  in.nr_items = RandomNumber(1, StockTxnInput::kStockMaxItems);

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

bool StockTxn_Run(PriorityTxn *txn)
{
  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t diff = start_tsc - (txn->delay + PriorityTxnService::g_tsc);
  probes::PriInitQueueTime{diff / 2200, txn->epoch, txn->delay}();

  // generate txn input
  StockTxnInput txnInput = dynamic_cast<tpcc::Client*>
      (EpochClient::g_workload_client)->GenerateTransactionInput<StockTxnInput>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < txnInput.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(txnInput.warehouse_id,
                                         txnInput.detail.item_id[i]));
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // init
  std::vector<VHandle*> stock_rows;
  for (auto key : stock_keys) {
    VHandle* row = nullptr;
    abort_if(!txn->InitRegisterUpdate<Stock>(key, row), "init register failed!");
    stock_rows.push_back(row);
  }
  uint64_t fail_tsc = start_tsc;
  int fail_cnt = 0;
  while (!txn->Init()) {
    fail_tsc = __rdtsc();
    ++fail_cnt;
  }

  struct Context {
    uint warehouse_id;
    uint nr_items;
    PriorityTxn *txn;
    uint stock_quantities[StockTxnInput::kStockMaxItems];
    VHandle* stock_rows[StockTxnInput::kStockMaxItems];
  };
  // issue promise
  auto lambda =
      [](std::tuple<Context> capture) {
        auto [ctx] = capture;
        auto piece_id = ctx.txn->piece_count.fetch_sub(1);

        // record exec queue time
        auto queue_tsc = __rdtsc();
        auto diff = queue_tsc - ctx.txn->measure_tsc;
        probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
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
        auto total = exec_tsc - (ctx.txn->delay + PriorityTxnService::g_tsc);
        probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
      };
  Context ctx{txnInput.warehouse_id,
              txnInput.nr_items,
              txn};
  memcpy(ctx.stock_quantities, txnInput.detail.stock_quantities, sizeof(uint) * ctx.nr_items);
  memcpy(ctx.stock_rows, &stock_rows[0], sizeof(VHandle*) * ctx.nr_items);
  txn->IssuePromise(ctx, lambda);
  // debug(TRACE_PRIORITY "Priority txn {:p} (stock) - Issued lambda into PQ", (void *)txn);

  uint64_t succ_tsc = __rdtsc();
  txn->measure_tsc = succ_tsc;
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  // debug(TRACE_PRIORITY "Priority txn {:p} (stock) - Init() succuess, sid {} - {}", (void *)txn, txn->serial_id(), format_sid(txn->serial_id()));
  probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();

  // record acquired SID's difference from current max progress
  uint64_t max_prog = util::Instance<PriorityTxnService>().GetMaxProgress() >> 8;
  uint64_t seq = txn->serial_id() >> 8;
  int64_t diff_to_max_progress = seq - max_prog;
  probes::Distance{diff_to_max_progress, txn->serial_id()}();

  return txn->Commit();
}

bool NewOrderDeliveryTxn_Run(PriorityTxn *txn)
{
  // record pri txn init queue time
  uint64_t start_tsc = __rdtsc();
  uint64_t diff = start_tsc - (txn->delay + PriorityTxnService::g_tsc);
  probes::PriInitQueueTime{diff / 2200, txn->epoch, txn->delay}();

  // generate txn input
  NewOrderStruct input = dynamic_cast<Client*>
      (EpochClient::g_workload_client)->GenerateTransactionInput<NewOrderStruct>();
  std::vector<Stock::Key> stock_keys;
  for (int i = 0; i < input.nr_items; ++i) {
    stock_keys.push_back(Stock::Key::New(input.detail.supplier_warehouse_id[i],
                                         input.detail.item_id[i]));
  }
  // hack, subtract random gen time
  start_tsc = __rdtsc();

  // register new order update
  std::vector<VHandle*> stock_rows;
  for (auto key : stock_keys) {
    VHandle* row = nullptr;
    abort_if(!txn->InitRegisterUpdate<Stock>(key, row), "init register failed!");
    stock_rows.push_back(row);
  }

  // register new order insert
  auto auto_inc_zone = input.warehouse_id * 10 + input.district_id;
  auto oorder_id = dynamic_cast<Client*>(EpochClient::g_workload_client)->
                   relation(OOrder::kTable).AutoIncrement(auto_inc_zone);

  auto oorder_key = OOrder::Key::New(input.warehouse_id, input.district_id, oorder_id);
  auto neworder_key = NewOrder::Key::New(input.warehouse_id, input.district_id, oorder_id, input.customer_id);
  OrderLine::Key orderline_keys[input.kNewOrderMaxItems];
  for (int i = 0; i < input.nr_items; i++)
    orderline_keys[i] = OrderLine::Key::New(input.warehouse_id, input.district_id, oorder_id, i + 1);

  BaseInsertKey *oorder_ikey, *neworder_ikey, *orderline_ikeys[input.kNewOrderMaxItems];
  txn->InitRegisterInsert<OOrder>(oorder_key, oorder_ikey);
  txn->InitRegisterInsert<NewOrder>(neworder_key, neworder_ikey);
  for (int i = 0; i < input.nr_items; i++)
    txn->InitRegisterInsert<OrderLine>(orderline_keys[i], orderline_ikeys[i]);

  // register delivery update (insert rows' update doesn't need to be registered)
  auto customer_key = Customer::Key::New(input.warehouse_id, input.district_id, input.customer_id);
  VHandle *customer_row = nullptr;
  abort_if(!txn->InitRegisterUpdate<Customer>(customer_key, customer_row), "customer init fail");

  // init
  uint64_t fail_tsc = start_tsc;
  int fail_cnt = 0;
  while (!txn->Init()) {
    fail_tsc = __rdtsc();
    ++fail_cnt;
  }

  struct Context {
    NewOrderStruct in;
    PriorityTxn *txn;
    BaseInsertKey *oorder_ikey;
    BaseInsertKey *neworder_ikey;
    VHandle *customer_row;
    BaseInsertKey *orderline_ikeys[NewOrderStruct::kNewOrderMaxItems];
    VHandle *stock_rows[NewOrderStruct::kNewOrderMaxItems];
  };
  // issue promise
  auto lambda =
      [](std::tuple<Context> capture) {
        auto [ctx] = capture;
        auto piece_id = ctx.txn->piece_count.fetch_sub(1);

        // record exec queue time
        auto queue_tsc = __rdtsc();
        auto diff = queue_tsc - ctx.txn->measure_tsc;
        probes::PriExecQueueTime{diff / 2200, ctx.txn->serial_id()}();
        ctx.txn->measure_tsc = queue_tsc;

        // new order - update stock
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
        // new order - update (inserted) oorder, neworder
        // actually can't write here, because final write of these rows are in the delivery part
        auto oorder_value = OOrder::Value::New(ctx.in.customer_id, 0, ctx.in.nr_items,
                                               all_local, ctx.in.ts_now);
        // ctx.txn->Write(ctx.txn->InsertKeyToVHandle(ctx.oorder_ikey), oorder_value);
        auto neworder_value = NewOrder::Value();
        // ctx.txn->Write(ctx.txn->InsertKeyToVHandle(ctx.neworder_ikey), NewOrder::Value());

        // new order - update (inserted) orderline
        OrderLine::Value ol_values[ctx.in.nr_items];
        for (int i = 0; i < ctx.in.nr_items; ++i) {
          auto item = util::Instance<RelationManager>().Get<Item>().
                      Search(Item::Key::New(ctx.in.detail.item_id[i]).EncodeFromRoutine());
          auto item_value = ctx.txn->Read<Item::Value>(item);
          auto amount = item_value.i_price * ctx.in.detail.order_quantities[i];
          ol_values[i] = OrderLine::Value::New(ctx.in.detail.item_id[i], 0, amount,
                                               ctx.in.detail.supplier_warehouse_id[i],
                                               ctx.in.detail.order_quantities[i]);
          // ctx.txn->Write(ctx.txn->InsertKeyToVHandle(ctx.orderline_ikeys[i]), ol_values[i]);
        }

        // delivery - delete neworder
        ctx.txn->Delete(ctx.txn->InsertKeyToVHandle(ctx.neworder_ikey));
        // delivery - update oorder
        auto oorder_row = ctx.txn->InsertKeyToVHandle(ctx.oorder_ikey);
        oorder_value.o_carrier_id = __rdtsc() % 10 + 1; // random between 1 and 10
        ctx.txn->Write(oorder_row, oorder_value);
        ClientBase::OnUpdateRow(oorder_row);
        // delivery - update orderline
        int sum = 0;
        for (int i = 0; i < ctx.in.nr_items; ++i) {
          auto orderline_row = ctx.txn->InsertKeyToVHandle(ctx.orderline_ikeys[i]);
          sum += ol_values[i].ol_amount;
          ol_values[i].ol_delivery_d = 234567; /* ts */
          ctx.txn->Write(orderline_row, ol_values[i]);
          ClientBase::OnUpdateRow(orderline_row);
        }
        // delivery - update customer
        auto cust = ctx.txn->Read<Customer::Value>(ctx.customer_row);
        cust.c_balance += sum;
        cust.c_delivery_cnt++;
        ctx.txn->Write(ctx.customer_row, cust);
        ClientBase::OnUpdateRow(ctx.customer_row);

        // record exec time
        auto exec_tsc = __rdtsc();
        auto exec = exec_tsc - ctx.txn->measure_tsc;
        auto total = exec_tsc - (ctx.txn->delay + PriorityTxnService::g_tsc);
        probes::PriExecTime{exec / 2200, total / 2200, ctx.txn->serial_id()}();
      };
  Context ctx {input, txn, oorder_ikey, neworder_ikey, customer_row};
  memcpy(ctx.orderline_ikeys, orderline_ikeys, sizeof(BaseInsertKey) * input.kNewOrderMaxItems);
  memcpy(ctx.stock_rows, &stock_rows[0], sizeof(VHandle*) * input.nr_items);
  txn->IssuePromise(ctx, lambda);

  uint64_t succ_tsc = __rdtsc();
  txn->measure_tsc = succ_tsc;
  uint64_t fail = fail_tsc - start_tsc, succ = succ_tsc - fail_tsc;
  probes::PriInitTime{succ / 2200, fail / 2200, fail_cnt, txn->serial_id()}();

  // record acquired SID's difference from current max progress
  uint64_t max_prog = util::Instance<PriorityTxnService>().GetMaxProgress() >> 8;
  uint64_t seq = txn->serial_id() >> 8;
  int64_t diff_to_max_progress = seq - max_prog;
  probes::Distance{diff_to_max_progress, txn->serial_id()}();

  return txn->Commit();
}

}
