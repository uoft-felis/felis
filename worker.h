// -*- c++ -*-

#ifndef WORKER_H
#define WORKER_H

#include <sys/types.h>
#include <pthread.h>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace dolly {

class Txn;

// each worker represent one thread
class Worker {
public:
  static int kNrThreads;

  static void PinCurrentThread(int cpu_id);
  static int CurrentThreadId() {
    return pthread_self();
  }
  static Worker *CurrentThreadWorker();

  Worker() : data_ptr(nullptr) {}
  Worker(Worker &&rhs) : data_ptr(rhs.data_ptr) {
    rhs.data_ptr = nullptr;
  }

  void Start();

  void AddTask(std::function<void ()> func, bool batch = false);

  template <typename T>
  T *worker_data() const { return static_cast<T *>(data_ptr); }

  template <typename T>
  void set_worker_data(T *p) { data_ptr = p; }

  int index() const { return thread_index; }
  void set_index(int tindex) { thread_index = tindex; }

private:
  std::queue<std::function<void ()> > task_queue;
  std::mutex mutex;
  std::condition_variable cond;

  void *data_ptr;
  int thread_index; // mainly used for threadinfo in masstree
};

class WorkerManager {
  int current_worker;
  std::vector<Worker> workers;
public:
  WorkerManager();
  Worker &SelectWorker() {
    return workers[current_worker++ % workers.size()];
  }
  Worker &GetWorker(int hash) { return workers[hash % workers.size()]; }
};

void InitWorkerManager();

}

#endif /* WORKER_H */
