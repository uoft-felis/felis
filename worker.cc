#include <sched.h>
#include "numa.h"
#include "worker.h"
#include "util.h"
#include "log.h"

namespace dolly {

int Worker::kNrThreads = 16; // TODO: get number of CPUs

void Worker::PinCurrentThread(int cpu_id)
{
  auto node = numa_node_of_cpu(cpu_id);
  numa_run_on_node(node);
  sched_yield();
}

std::future<void> Worker::AddTask(std::function<void ()> func)
{
  std::lock_guard<std::mutex> _(mutex);
  std::packaged_task<void()> task(func);
  auto future = task.get_future();
  task_queue.emplace(std::move(task));
  cond.notify_all();
  return future;
}

void Worker::Start()
{
  std::thread th([this]() {
      logger->info("worker thread started, {}", (void *) this);
      try {
	while (true) {
	  std::unique_lock<std::mutex> lock(mutex);
	  while (task_queue.empty()) {
	    cond.wait(lock);
	  }
	  std::packaged_task<void()> task(std::move(task_queue.front()));
	  task_queue.pop();
	  lock.unlock();

	  task();
	}
      } catch (std::system_error &ex) {
	logger->critical(ex.what());
      }
    });
  th.detach();
}

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
