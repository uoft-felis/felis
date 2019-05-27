#ifndef MODULE_H
#define MODULE_H

#include <string>
#include <cstdio>

namespace felis {

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
  bool initialized = false;
 protected:
  bool required = false;
  struct {
    std::string name;
    std::string description;
  } info;
  Module();
  virtual void Init() = 0;
 public:
  void Load() {
    if (initialized)
      return;

    printf("Loading %s module...\n", info.name.c_str());
    fflush(stdout);
    Init();
    initialized = true;
  }

  static void InitRequiredModules();
  static void InitModule(std::string name);
  static void ShowAllModules();
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
void Module<Type>::InitRequiredModules()
{
  for (auto p = head(); p; p = p->next) {
    if (p->required)
      p->Load();
  }
}

template <int Type>
void Module<Type>::ShowAllModules()
{
  for (auto p = head(); p; p = p->next) {
    printf("Found module %s%s: %s\n",
           p->info.name.c_str(),
           p->required ? "(required)" : "",
           p->info.description.c_str());
  }
}

template <int Type>
void Module<Type>::InitModule(std::string name)
{
  for (auto p = head(); p; p = p->next) {
    if (p->info.name == name) {
      p->Load();
      return;
    }
  }
  printf("Cannot find workload module %s\n", name.c_str());
  std::abort();
}

}

#endif /* MODULE_H */
