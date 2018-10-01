#include <atomic>
#include <functional>

namespace felis {

/** We call this value completion.
 *
 * For some transactions, a few write keys is not possible to infer from just
 * the input parameters. Usually, we need to read some value from the database
 * to infer the full write-set keys. However, transactions are not fully
 * processed and these values need to be read are mostly dummy values at the
 * moment.
 *
 * In this case, we first put a dummy for the entire range. This means that this
 * transaction is going to write to any keys in this range during
 * execution. Then, we add completion  objects to the values we need to read to
 * infer the full write-set. As soon as these values are filled, we execute the
 * function callbacks in the completion objects as soon as possible.
 *
 * TODO: speed up memory allocation using a memory pool!
 */
class CompletionObject {
  std::atomic_ulong comp_count;
  std::function<void ()> callback;
 public:
  CompletionObject(long count, std::function<void ()> callback)
      : comp_count(count), callback(callback) {}

  void Complete() {
    if (comp_count.fetch_sub(1) == 1) {
      callback();
    }
  }
};

}
