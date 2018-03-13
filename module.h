#ifndef MODULE_H
#define MODULE_H

#include <string>
#include <cstdio>
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
  static void ShowModules();
};

template <int Type>
Module<Type>::Module()
{
  Module<Type> **p = &head();
  while (*p) p = &((*p)->next);
  *p = this;
  next = nullptr;
}

template <int Type>
void Module<Type>::InitAllModules()
{
  auto p = head();
  while (p) {
    logger->info("Loading {} module", p->name());
    p->Init();
    logger->info("{} module initialized", p->name());
    p = p->next;
  }
}

template <int Type>
void Module<Type>::ShowModules()
{
  auto p = head();
  while (p) {
    logger->info("Found module {}", p->name());
    p = p->next;
  }
}

}

#endif /* MODULE_H */
