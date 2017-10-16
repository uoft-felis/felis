#ifndef CLIENT_H
#define CLIENT_H

#include <string>
#include "gopp/gopp.h"
#include "gopp/channels.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"
#include "log.h"

namespace dolly {

class ClientFetcher : public go::Routine {
  bool replay_from_file; // contact a local webserver
  go::BufferChannel *epoch_ch;
  std::string workload_name;

  PerfLog *p;
  int timer_skip_epoch;
  int timer_force_terminate;

  // static const char *kDummyWebServerHost = "127.0.0.1";
  static const std::string kDummyWebServerHost;
  static const int kDummyWebServerPort = 8000;

 public:
  ClientFetcher(go::BufferChannel *ch, std::string name)
      : epoch_ch(ch), workload_name(name), p(nullptr), timer_skip_epoch(30), timer_force_terminate(-1) {
    set_reuse(true);
    set_share(true);
  }

  void set_replay_from_file(bool b) { replay_from_file = b; }
  void set_timer_skip_epoch(int s) { timer_skip_epoch = s; }
  void set_timer_force_terminate(int t) { timer_force_terminate = t; }
  PerfLog *perf_log() { return p; }

  virtual void Run();
};

class ClientExecutor : public go::Routine {
  go::BufferChannel *epoch_ch;
  std::mutex *mp;
  ClientFetcher *fetcher;
 public:
  ClientExecutor(go::BufferChannel *ch, std::mutex *m, ClientFetcher *cf)
      : epoch_ch(ch), mp(m), fetcher(cf) {
    set_share(true);
  }

  virtual void Run();
};

}

#endif /* CLIENT_H */
