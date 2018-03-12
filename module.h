#ifndef MODULE_H
#define MODULE_H

#include <string>
#include "log.h"

namespace dolly {

enum ModuleType {
  CoreModule,
  WorkloadModule,
  ExportModule,
};

template <int Type>
class Module {
  static Module<Type> *&head() {
    static Module<Type> *head = nullptr;
    return head;
  }
  Module<Type> *next;
 public:
  Module();
  virtual void Init() = 0;
  virtual std::string name() const { return std::string("NoName"); }
  static void InitAllModules();
};

template <int Type>
Module<Type>::Module()
{
  next = head();
  head() = this;
}

template <int Type>
void Module<Type>::InitAllModules()
{
  auto p = head();
  while (p) {
    logger->info("Loading {} module", p->name());
    p->Init();
    p = p->next;
    logger->info("{} module initialized", p->name());
  }
}

}

#endif /* MODULE_H */
