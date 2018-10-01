#include <string>
#include <cstdlib>
#include "fpcore.h"

namespace felis {

struct POData {
  int a;
  int b;
};

// __attribute__((constructor))
void foo()
{
  std::string b[] = {
    std::string("one"),
    std::string("two"),
    std::string("three"),
    std::string("four"),
  };
  int a[] = {1, 2, 3, 4};
  auto v1 = MakeVector(a, 4);
  auto v2 = MakeVector(b, 4);
  auto v = MakeVector(Map(Zip(v1, v2), [](Tuple<int, std::string> t) -> int {
        return (int) std::get<0>(t);
      }));
  for (int i = 0; i < v.length(); i++) {
    printf("%d\n", v[i]);
  }

  POData d = {0, 0};
  auto t = Transient<POData>(&d);
  t.Update()->a = 1;
  t.Update()->b = 10;
  POData *newd = t.Persist();
  printf("oldd %d %d\n", d.a, d.b);
  printf("newd %d %d\n", newd->a, newd->b);

  auto vt = Transient<Vector<int>>(v1);
  vt.Update().Insert(10, 3);
  v1 = vt.Persist();
  for (int i = 0; i < v1.length(); i++) {
    printf("%d\n", v1[i]);
  }
  std::exit(0);
}

}
