#include <sched.h>
#include "numa.h"
#include "worker.h"
#include "util.h"
#include "log.h"

namespace dolly {

int Worker::kNrThreads = 16; // TODO: get number of CPUs

static __thread Worker *TLSWorker;

void Worker::PinCurrentThread(int cpu_id)
{
  auto node = numa_node_of_cpu(cpu_id);
  numa_run_on_node(node);
  sched_yield();
}

void Worker::AddTask(std::function<void ()> func, bool batch)
{
  std::lock_guard<std::mutex> _(mutex);
  task_queue.emplace(std::move(func));
  if (!batch || task_queue.size() > 1024)
    cond.notify_one();
}

void Worker::Start()
{
  std::thread th([this]() {
      TLSWorker = this;
      logger->info("worker thread started, {}", (void *) this);
      try {
	while (true) {
	  std::unique_lock<std::mutex> lock(mutex);
	  while (task_queue.empty()) {
	    cond.wait(lock);
	  }
	  auto task = task_queue.front();
	  task_queue.pop();
	  lock.unlock();

	  task();
	}
      } catch (std::system_error &ex) {
	logger->critical(ex.what());
      }
      TLSWorker = nullptr;
    });
  th.detach();
}

Worker *Worker::CurrentThreadWorker() { return TLSWorker; }

WorkerManager::WorkerManager()
{
  int nthreads = Worker::kNrThreads;
  // int nthreads = 1;
  for (int i = 0; i < nthreads; i++) {
    workers.push_back(std::move(Worker()));
  }
  for (auto &worker: workers) {
    worker.Start();
  }
}

}

// exports global variable
namespace util {

template <>
dolly::WorkerManager &Instance()
{
  static dolly::WorkerManager mgr;
  return mgr;
}

}
