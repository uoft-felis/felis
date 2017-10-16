#ifndef API_H
#define API_H

#include <functional>
#include <string>

namespace dolly {

// TODO: finish this
template <typename T>
class Entity : public typename T::KeyStruct, public typename T::ValueStruct {
  const VarStr *value_ptr = null;
 public:
  static int relation_id() { return T::kRelationId; }
};

}

#endif /* API_H */
