#ifndef TCP_NODE_H
#define TCP_NODE_H

#include "node_config.h"
#include "promise.h"

namespace felis {
namespace tcp {
class NodeServerRoutine;
}

class TcpNodeTransport : public PromiseRoutineTransportService {
  static NodeConfiguration &node_config() {
    return util::Instance<NodeConfiguration>();
  }
  // std::array<tcp::NodeConnectionRoutine *, kMaxNrNode> connection_routines;
  // std::array<tcp::SendChannel *, kMaxNrNode> out_channels;
  tcp::NodeServerRoutine *serv;
  LocalTransport ltp;
  std::atomic_int counters = 0;
 public:
  TcpNodeTransport();

  void TransportPromiseRoutine(PromiseRoutine *routine, const VarStr &in) final override;
  void PreparePromisesToQueue(int core, int level, unsigned long nr) final override;
  void FinishPromiseFromQueue(PromiseRoutine *routine) final override;
  bool PeriodicIO(int core) final override;
  uint8_t GetNumberOfNodes() final override {
    return node_config().nr_nodes();
  }

  void OnCounterReceived();
};

}

namespace util {

template <>
struct InstanceInit<felis::TcpNodeTransport> {
  static constexpr bool kHasInstance = true;
  static inline felis::TcpNodeTransport *instance;
  InstanceInit() {
    instance = new felis::TcpNodeTransport();
  }
};

}

#endif
