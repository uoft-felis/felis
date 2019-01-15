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

void IndexShipmentReceiver::Run()
{
  // clear the affinity
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

  IndexEntity ent;
  auto &mgr = Instance<RelationManager>();

  logger->info("New Shipment has arrived!");
  PerfLog perf;
  while (Receive(&ent)) {
    // TODO: multi-thread this?
    auto &rel = mgr[ent.rel_id];
    auto handle = rel.InsertOrDefault(ent.k, [&ent]() { return ent.handle_ptr; });
  }
  logger->info("Shipment processing finished");
  perf.End();
  perf.Show("Processing takes");
  sock->Close();
}

IndexShipmentReceiver::~IndexShipmentReceiver()
{
  delete sock;
}

void DataSlicer::Initialize(int nr_slices)
{
  this->nr_slices = nr_slices;
  index_slices = new Slice*[nr_slices];
  index_slice_scanners = new IndexSliceScanner*[nr_slices];
  memset(index_slices, 0, sizeof(Slice *));
  memset(index_slice_scanners, 0, sizeof(IndexSliceScanner *));

  row_slices = new Slice*[nr_slices];
  row_slice_scanners = new RowSliceScanner*[nr_slices];
  memset(row_slices, 0, sizeof(Slice *));
  memset(row_slices, 0, sizeof(RowSliceScanner *));
}

std::vector<IndexShipment *> DataSlicer::all_index_shipments()
{
  std::vector<IndexShipment *> all;
  for (int i = 0; i < nr_slices; i++) {
    if (index_slice_scanners[i] == nullptr)
      continue;
    auto shipment = index_slice_scanners[i]->shipment();
    if (shipment) all.push_back(shipment);
  }
  return all;
}

void InitVersion(felis::VHandle *handle, VarStr *obj = (VarStr *) kPendingValue) {
  while (!handle->AppendNewVersion(0, 0)) {
      asm("pause" : : :"memory");
    }
    if (obj != (void *) kPendingValue) {
      abort_if(!handle->WriteWithVersion(0, obj, 0),
               "Diverging outcomes during setup setup");
    }
}

}
