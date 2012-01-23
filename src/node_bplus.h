#ifndef _SRC_NODE_BPLUS_H_
#define _SRC_NODE_BPLUS_H_

#include <uv.h>
#include <node.h>
#include <node_object_wrap.h>
#include <node_buffer.h>
#include <v8.h>
#include <string.h>

#include <bplus.h>

namespace bplus {

using namespace node;
using namespace v8;

#ifndef offset_of
// g++ in strict mode complains loudly about the system offsetof() macro
// because it uses NULL as the base address.
#define offset_of(type, member) \
  ((intptr_t) ((char *) &(((type *) 8)->member) - 8))
#endif

#ifndef container_of
#define container_of(ptr, type, member) \
  ((type *) ((char *) (ptr) - offset_of(type, member)))
#endif

void BufferToKey(Handle<Object> obj, bp_key_t* key) {
  key->length = Buffer::Length(obj);
  key->value = new char[key->length];
  memcpy(key->value, Buffer::Data(obj), key->length);
}


class BPlus : ObjectWrap {
 public:
  struct bp_base_req {
    BPlus* b;
    uv_work_t w;

    bp_key_t key;
    bp_value_t value;

    int result;

    Persistent<Function> callback;
  };

  static void Initialize(Handle<Object> target);

  BPlus() : ObjectWrap(), opened_(false) {
    uv_mutex_init(&write_mutex_);
  }

  ~BPlus() {
    if (opened_) bp_close(&db_);
    uv_mutex_destroy(&write_mutex_);
  }

 protected:
  static Handle<Value> New(const Arguments &args);
  static Handle<Value> Open(const Arguments &args);
  static Handle<Value> Close(const Arguments &args);

  static void DoSet(uv_work_t* work);
  static void AfterSet(uv_work_t* work);
  static Handle<Value> Set(const Arguments &args);

  static void DoGet(uv_work_t* work);
  static void AfterGet(uv_work_t* work);
  static Handle<Value> Get(const Arguments &args);

  bool opened_;
  bp_tree_t db_;

  uv_mutex_t write_mutex_;
};

} // namespace plus

#endif // _SRC_NODE_BPLUS_H_
