// -*- c++ -*-

#ifndef WORKER_H
#define WORKER_H

#include <sys/types.h>
#include <pthread.h>
#include <vector>
#include <queue>
#include <thread>
#include <functional>
#include <future>

namespace dolly {

class Txn;

class Worker {
public:
  static int kNrThreads;

  static void PinCurrentThread(int cpu_id);
  static int CurrentThreadId() {
    return pthread_self();
  }

  Worker() {}
  Worker(Worker &&rhs) {}

  void Start();

  std::future<void> AddTask(std::function<void ()> func);

private:
  std::queue<std::packaged_task<void()>> task_queue;
  std::mutex mutex;
  std::condition_variable cond;
};

class WorkerManager {
  int current_worker;
  std::vector<Worker> workers;
public:
  WorkerManager();
  Worker &SelectWorker() {
    return workers[current_worker++ % workers.size()];
  }
};

}

#endif /* WORKER_H */
