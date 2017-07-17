#ifndef MODULE_H
#define MODULE_H

namespace dolly {

enum ModuleType {
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
    p->Init();
    p = p->next;
  }
}

}

#endif /* MODULE_H */
