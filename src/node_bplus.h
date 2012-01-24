#ifndef _SRC_NODE_BPLUS_H_
#define _SRC_NODE_BPLUS_H_

#include <uv.h>
#include <node.h>
#include <node_object_wrap.h>
#include <node_buffer.h>
#include <v8.h>
#include <string.h>
#include <assert.h>

#include <bplus.h>

namespace bplus {

using namespace node;
using namespace v8;

#ifndef offset_of
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

void BufferToRawValue(Handle<Object> obj, bp_value_t* value) {
  assert(Buffer::Length(obj) == sizeof(*value));
  memcpy((char*) value, Buffer::Data(obj), Buffer::Length(obj));
}

Handle<Object> ValueToObject(bp_value_t* v) {
  Handle<Object> result = Object::New();

  result->Set(String::NewSymbol("value"),
              Buffer::New(v->value, v->length)->handle_);
  result->Set(String::NewSymbol("ref"),
              Buffer::New((char*) v, sizeof(*v))->handle_);

  return result;
}


class BPlus : ObjectWrap {
 public:
  enum bp_work_type {
    kSet,
    kGet,
    kRemove,
    kCompact,
    kGetPrevious
  };

  struct bp_work_req {
    BPlus* b;
    uv_work_t w;

    enum bp_work_type type;

    union {
      struct {
        bp_key_t key;
        bp_value_t value;
      } set;

      struct {
        bp_key_t key;
        bp_value_t value;
      } get;

      struct {
        bp_key_t key;
      } remove;

      struct {
        bp_value_t value;
        bp_value_t previous;
      } previous;
    } data;

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

  static void DoWork(uv_work_t* work);
  static void AfterWork(uv_work_t* work);

  static Handle<Value> Set(const Arguments &args);
  static Handle<Value> Get(const Arguments &args);
  static Handle<Value> Remove(const Arguments &args);
  static Handle<Value> Compact(const Arguments &args);
  static Handle<Value> GetPrevious(const Arguments &args);

  bool opened_;
  bp_tree_t db_;

  uv_mutex_t write_mutex_;
};

} // namespace plus

#endif // _SRC_NODE_BPLUS_H_
