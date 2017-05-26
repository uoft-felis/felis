#include <sstream>
#include "client.h"
#include "gopp/channels.h"
#include "log.h"

namespace dolly {

const std::string ClientFetcher::kDummyWebServerHost = "142.150.234.190";

void ClientFetcher::Run()
{
  go::BufferChannel chn(Epoch::kNrThreads);
  std::vector<go::TcpSocket *> socks;
  socks.assign(Epoch::kNrThreads, nullptr);

  for (int i = 0; i < Epoch::kNrThreads; i++) {
    auto sched = go::GetSchedulerFromPool(i + 1);
    sched->WakeUp(go::Make([i, &socks, &chn, this] {
          auto sock = new go::TcpSocket(16 << 20, 4096);
          if (!sock->Pin()) {
            logger->critical("pin socket failed on {}", go::Scheduler::CurrentThreadPoolId());
          }
          sock->Connect(kDummyWebServerHost, kDummyWebServerPort);

          if (replay_from_file) {
            auto out = sock->output_channel();
            std::stringstream ss;
            ss << "GET /" << workload_name << "/" << Epoch::kNrThreads
               << "/dolly-net." << i << ".dump HTTP/1.0\r\n\r\n";
            out->Write(ss.str().c_str(), ss.str().length());
            // fprintf(stderr, "%s\n", ss.str().c_str());
            out->Flush();
            auto in = sock->input_channel();
            uint8_t ch;
            int line_len = 0;
            while (true) {
              bool rs = in->Read(&ch, 1);
              if (!rs) {
                logger->critical("server returned EOF prematurely");
                std::abort();
              }
              if (ch == '\r') {
                continue;
              } else if (ch == '\n') {
                if (line_len == 0) break;
                line_len = 0;
              } else {
                line_len++;
              }
            }
          }

          socks[i] = sock;
          uint8_t u = 1;
          chn.Write(&u, 1);
        }));
  }
  for (int i = 0; i < Epoch::kNrThreads; i++) {
    uint8_t u;
    chn.Read(&u, 1);
  }

  // set_urgent(true);
  try {
    int nr = 0;
    while (true) {
      logger->info("firing up an epoch, cnt {}", nr + 1);

      if (++nr == timer_skip_epoch + 1) p = new PerfLog();

      auto epoch = new Epoch(socks);
      logger->info("received a complete epoch");
      epoch_ch->Write(&epoch, sizeof(Epoch *));
      epoch_ch->Flush();
    }
  } catch (dolly::ParseBufferEOF &ex) {
    logger->info("EOF");
    epoch_ch->Flush();
    epoch_ch->Close();
  }
  fprintf(stderr, "exiting from %d\n", go::Scheduler::CurrentThreadPoolId());
}

void ClientExecutor::Run()
{
  // set_urgent(true);
  while (true) {
    logger->info("executor waiting...");
    Epoch *epoch = nullptr;
    bool success = epoch_ch->Read(&epoch, sizeof(Epoch *));
    if (!success) {
      mp->unlock();
      break;
    }
    logger->info("issueing...");
    int nr_wait = epoch->IssueReExec();
    epoch->WaitForReExec(nr_wait);
    logger->info("all done for the epoch");
    delete epoch;
  }
  fetcher->perf_log()->Show("Epoch Executor total");
}

}
