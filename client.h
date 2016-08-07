#ifndef CLIENT_H
#define CLIENT_H

#include "goplusplus/gopp.h"
#include "epoch.h"
#include "util.h"
#include "mem.h"

namespace dolly {

class ClientFetcher : public go::Routine {
  bool replay_from_file; // contact a local webserver
  int *peer_fds;
  go::BufferChannel<Epoch *> *epoch_ch;
public:
  ClientFetcher(int *fds, go::BufferChannel<Epoch *> *ch) : peer_fds(fds), epoch_ch(ch) {
    set_share(true);
  }

  void set_replay_from_file(bool b) { replay_from_file = b; }

  virtual void Run();
};

class ClientExecutor : public go::Routine {
  go::BufferChannel<Epoch *> *epoch_ch;
  std::mutex *mp;
public:
  ClientExecutor(go::BufferChannel<Epoch *> *ch, std::mutex *m) : epoch_ch(ch), mp(m) {
    /* set_share(true); */
  }

  virtual void Run();
};

}

#endif /* CLIENT_H */
