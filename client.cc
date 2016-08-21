#include <sstream>
#include "client.h"
#include "log.h"

namespace dolly {

void ClientFetcher::Run()
{
  try {
    std::vector<go::EpollSocket *> socks;

    for (int i = 0; i < dolly::Epoch::kNrThreads; i++) {
      auto sock = new go::EpollSocket(peer_fds[i], go::GlobalEpoll(),
				      new go::InputSocketChannel(16 << 20),
				      new go::OutputSocketChannel(4096));
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
	  bool rs = in->Read(&ch);
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
      socks.push_back(sock);
    }
    set_urgent(true);

    int nr = 0;
    while (true) {
      logger->info("firing up an epoch");

      if (++nr == 31) p = new PerfLog();

      auto epoch = new dolly::Epoch(socks);
      logger->info("received a complete epoch");
      epoch_ch->Write(epoch);
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
  set_urgent(true);
  while (true) {
    bool eof = false;
    logger->info("executor waiting...");
    auto epoch = epoch_ch->Read(eof);
    if (eof) {
      mp->unlock();
      break;
    }
    logger->info("issueing...");
    epoch->IssueReExec();
    epoch->WaitForReExec();
    logger->info("all done for the epoch");
    delete epoch;
  }
  fetcher->perf_log()->Show("Epoch Executor total");
}

}
