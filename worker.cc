#include <sched.h>
#include <pthread.h>
#include "numa.h"
#include "worker.h"
#include "util.h"
#include "log.h"

namespace dolly {

int Worker::kNrThreads = 16; // TODO: get number of CPUs

static __thread Worker *TLSWorker;

void Worker::PinCurrentThread(int cpu_id)
{
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu_id * 2, &set);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &set);
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

WorkerManager *mgr;

void InitWorkerManager()
{
  mgr = new WorkerManager();
}

}

// exports global variable
namespace util {

template <>
dolly::WorkerManager &Instance()
{
  return *dolly::mgr;
}

}
