#ifndef TCP_NODE_H
#define TCP_NODE_H

#include "node_config.h"
#include "piece.h"
#include "gopp/gopp.h"
#include "gopp/channels.h"

namespace felis {
namespace tcp {
class NodeServerRoutine;
class ReceiverChannel;
class SendChannel;
}

class TcpNodeTransport : public PromiseRoutineTransportService {
  static NodeConfiguration &node_config() {
    return util::Instance<NodeConfiguration>();
  }
  friend class tcp::NodeServerRoutine;

  tcp::NodeServerRoutine *serv;

  std::array<go::TcpSocket *, kMaxNrNode> incoming_socks;
  std::array<go::TcpSocket *, kMaxNrNode> outgoing_socks;

  std::array<tcp::SendChannel *, kMaxNrNode> outgoing_channels;
  std::array<tcp::ReceiverChannel *, kMaxNrNode> incoming_connection;

  LocalTransport ltp;
  std::atomic_int counters = 0;
 public:
  TcpNodeTransport();

  void TransportPromiseRoutine(PieceRoutine *routine) final override;
  void FinishCompletion(int level) final override;
  bool PeriodicIO(int core) final override;
  void PrefetchInbound() final override;
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
