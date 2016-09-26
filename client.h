#ifndef CLIENT_H
#define CLIENT_H

#include <string>
#include "goplusplus/gopp.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"
#include "log.h"

namespace dolly {

class ClientFetcher : public go::Routine {
  bool replay_from_file; // contact a local webserver
  int *peer_fds;
  go::BufferChannel<Epoch *> *epoch_ch;
  std::string workload_name;

  PerfLog *p;
  int timer_skip_epoch;
public:
  ClientFetcher(int *fds, go::BufferChannel<Epoch *> *ch, std::string name)
    : peer_fds(fds), epoch_ch(ch), workload_name(name), p(nullptr), timer_skip_epoch(30) {
    set_share(true);
  }

  void set_replay_from_file(bool b) { replay_from_file = b; }
  void set_timer_skip_epoch(int s) { timer_skip_epoch = s; }
  PerfLog *perf_log() { return p; }

  virtual void Run();
};

class ClientExecutor : public go::Routine {
  go::BufferChannel<Epoch *> *epoch_ch;
  std::mutex *mp;
  ClientFetcher *fetcher;
public:
  ClientExecutor(go::BufferChannel<Epoch *> *ch, std::mutex *m, ClientFetcher *cf)
    : epoch_ch(ch), mp(m), fetcher(cf) {
    // set_share(true);
  }

  virtual void Run();
};

}

#endif /* CLIENT_H */
