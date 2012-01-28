#include "node_bplus.h"

#include <uv.h>
#include <node.h>
#include <node_object_wrap.h>
#include <node_buffer.h>
#include <v8.h>
#include <stdlib.h>

#include <bplus.h>

#define UNWRAP\
    BPlus *b = ObjectWrap::Unwrap<BPlus>(args.Holder());

#define CHECK_OPENED(b)\
    if (!b->opened_) {\
      return ThrowException(String::New("Database wasn't opened"));\
    }

#define INIT_REQ(b_, type_, callback_)\
    Local<Function> fn = callback_.As<Function>();\
    Persistent<Function> callback = Persistent<Function>::New(fn);\
    bp_work_req* req = new bp_work_req;\
    req->b = b_;\
    req->type = type_;\
    req->callback = callback;\
    req->w.data = reinterpret_cast<void*>(req);\

#define QUEUE_WORK(b_, type_, callback_, prepare_)\
    INIT_REQ(b_, type_, callback_)\
    b->Ref();\
    prepare_\
    uv_queue_work(uv_default_loop(),\
                  &req->w,\
                  BPlus::DoWork,\
                  BPlus::AfterWork);

namespace bplus {

using namespace node;
using namespace v8;


void BufferToKey(Handle<Object> obj, bp_key_t* key) {
  key->length = Buffer::Length(obj);
  key->value = new char[key->length];
  memcpy(key->value, Buffer::Data(obj), key->length);
}


void BufferToRawValue(Handle<Object> obj, bp_value_t* value) {
  assert(Buffer::Length(obj) == sizeof(*value));
  memcpy(reinterpret_cast<char*>(value),
         Buffer::Data(obj),
         Buffer::Length(obj));
}


Handle<Value> ValueToObject(bp_value_t* v) {
  HandleScope scope;

  Local<Object> result = Object::New();

  result->Set(String::NewSymbol("value"),
              Buffer::New(v->value, v->length)->handle_);
  result->Set(String::NewSymbol("ref"),
              Buffer::New(reinterpret_cast<char*>(v), sizeof(*v))->handle_);

  return scope.Close(result);
}


void CopyBulkData(BPlus::bp_work_req* req, uint32_t len, Handle<Array> bulk) {
  req->data.bulk.length = len;
  req->data.bulk.keys = new bp_key_t[len];
  req->data.bulk.values = new bp_key_t[len];

  for (uint32_t i = 0; i < len; i++) {
    Local<Object> item = bulk->Get(i).As<Object>();
    BufferToKey(item->Get(String::NewSymbol("key")).As<Object>(),
                &req->data.bulk.keys[i]);
    BufferToKey(item->Get(String::NewSymbol("value")).As<Object>(),
                &req->data.bulk.values[i]);
  }
}


void DestroyBulkData(BPlus::bp_work_req* req) {
  for (uint64_t i = 0; i < req->data.bulk.length; i++) {
    free(req->data.bulk.keys[i].value);
    free(req->data.bulk.values[i].value);
  }

  free(req->data.bulk.keys);
  free(req->data.bulk.values);
}


Handle<Value> InvokeCallback(Handle<Object> obj,
                             Handle<Function> cb,
                             int c,
                             Handle<Value> argv[]) {
  HandleScope scope;
  Local<Value> result;
  TryCatch try_catch;

  result = cb->Call(obj, c, argv);

  if (try_catch.HasCaught()) {
    FatalException(try_catch);
    return Undefined();
  }

  return scope.Close(result);
}


void BPlus::Initialize(Handle<Object> target) {
  Local<FunctionTemplate> t = FunctionTemplate::New(BPlus::New);

  t->InstanceTemplate()->SetInternalFieldCount(1);
  t->SetClassName(String::NewSymbol("BPlus"));

  NODE_SET_PROTOTYPE_METHOD(t, "open", BPlus::Open);
  NODE_SET_PROTOTYPE_METHOD(t, "close", BPlus::Close);

  NODE_SET_PROTOTYPE_METHOD(t, "set", BPlus::Set);
  NODE_SET_PROTOTYPE_METHOD(t, "bulkSet", BPlus::BulkSet);
  NODE_SET_PROTOTYPE_METHOD(t, "update", BPlus::Update);
  NODE_SET_PROTOTYPE_METHOD(t, "bulkUpdate", BPlus::BulkUpdate);
  NODE_SET_PROTOTYPE_METHOD(t, "get", BPlus::Get);
  NODE_SET_PROTOTYPE_METHOD(t, "remove", BPlus::Remove);
  NODE_SET_PROTOTYPE_METHOD(t, "removev", BPlus::RemoveV);
  NODE_SET_PROTOTYPE_METHOD(t, "compact", BPlus::Compact);
  NODE_SET_PROTOTYPE_METHOD(t, "getPrevious", BPlus::GetPrevious);
  NODE_SET_PROTOTYPE_METHOD(t, "getRange", BPlus::GetRange);
  NODE_SET_PROTOTYPE_METHOD(t, "getFilteredRange", BPlus::GetFilteredRange);

  target->Set(String::NewSymbol("BPlus"), t->GetFunction());
}


Handle<Value> BPlus::New(const Arguments &args) {
  HandleScope scope;

  BPlus *b = new BPlus();
  b->Wrap(args.Holder());

  return args.This();
}


Handle<Value> BPlus::Open(const Arguments &args) {
  HandleScope scope;

  UNWRAP

  if (b->opened_) {
    return ThrowException(String::New("Database is already opened"));
  }

  String::Utf8Value v(args[0]->ToString());

  int ret = bp_open(&b->db_, *v);
  if (ret == BP_OK) {
    b->opened_ = true;
    return True();
  } else {
    return False();
  }
}


Handle<Value> BPlus::Close(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  int ret = bp_close(&b->db_);
  if (ret == BP_OK) {
    b->opened_ = false;
    return True();
  } else {
    return False();
  }
}


void BPlus::DoWork(uv_work_t* work) {
  bp_work_req* req = container_of(work, bp_work_req, w);
  switch (req->type) {
   case kSet:
    req->result = bp_set(&req->b->db_,
                         &req->data.set.key,
                         &req->data.set.value);

    free(req->data.set.key.value);
    free(req->data.set.value.value);
    break;
   case kBulkSet:
    req->result = bp_bulk_set(
        &req->b->db_,
        req->data.bulk.length,
        const_cast<const bp_key_t**>(&req->data.bulk.keys),
        const_cast<const bp_value_t**>(&req->data.bulk.values));

    DestroyBulkData(req);
    break;
   case kGet:
    req->result = bp_get(&req->b->db_,
                         &req->data.get.key,
                         &req->data.get.value);
    free(req->data.get.key.value);
    req->data.get.key.value = NULL;
    break;
   case kGetPrevious:
    req->result = bp_get_previous(&req->b->db_,
                                  &req->data.previous.value,
                                  &req->data.previous.previous);
    break;
   case kGetRange:
    req->result = bp_get_range(&req->b->db_,
                               &req->data.range.start,
                               &req->data.range.end,
                               BPlus::GetRangeCallback,
                               reinterpret_cast<void*>(req));
    free(req->data.range.start.value);
    free(req->data.range.end.value);
    break;
   case kRemove:
    req->result = bp_remove(&req->b->db_, &req->data.remove.key);

    free(req->data.remove.key.value);
    break;
   case kCompact:
    req->result = bp_compact(&req->b->db_);
    break;
   default:
    break;
  }
}


void BPlus::AfterWork(uv_work_t* work) {
  HandleScope scope;

  bp_work_req* req = container_of(work, bp_work_req, w);

  Handle<Value> args[2];

  if (req->result != BP_OK) {
    args[0] = True();
    args[1] = Number::New(req->result);
  } else {
    args[0] = Null();

    switch (req->type) {
     case kGet:
      args[1] = ValueToObject(&req->data.get.value);
      break;
     case kGetPrevious:
      args[1] = ValueToObject(&req->data.previous.previous);
      break;
     case kGetRange:
      req->data.range.queue->Push(new BPGetRangeMessage());
      uv_async_send(&req->data.range.notifier);
      break;
     default:
      args[1] = Undefined();
      break;
    }
  }

  if (req->type == kGetRange) return;

  InvokeCallback(req->b->handle_, req->callback, 2, args);

  req->callback.Dispose();
  req->callback.Clear();
  if (!req->filter.IsEmpty()) {
    req->filter.Dispose();
    req->filter.Clear();
  }
  req->b->Unref();

  delete req;
}


int BPlus::UpdateCallback(void* arg,
                          const bp_value_t* previous,
                          const bp_value_t* current) {
  bp_work_req* req = reinterpret_cast<bp_work_req*>(arg);
  Handle<Value> argv[2] = {
      ValueToObject(const_cast<bp_value_t*>(previous)),
      ValueToObject(const_cast<bp_value_t*>(current))
  };
  return !InvokeCallback(req->b->handle_, req->callback, 2, argv)->IsFalse();
}


int BPlus::RemoveCallback(void* arg,
                          const bp_value_t* value) {
  bp_work_req* req = reinterpret_cast<bp_work_req*>(arg);
  Handle<Value> argv[1] = { ValueToObject(const_cast<bp_value_t*>(value)) };
  return !InvokeCallback(req->b->handle_, req->callback, 1, argv)->IsFalse();
}


void BPlus::GetRangeCallback(void* arg,
                             const bp_key_t* key,
                             const bp_value_t* value) {
  bp_work_req* req = reinterpret_cast<bp_work_req*>(arg);

  req->data.range.queue->Push(new BPGetRangeMessage(key, value));

  uv_async_send(&req->data.range.notifier);
}


void BPlus::GetRangeNotifier(uv_async_t* async, int code) {
  bp_work_req* req = container_of(async, bp_work_req, data.range.notifier);
  BPGetRangeMessage* msg = NULL;

  while ((msg = req->data.range.queue->Shift()) != NULL) {
    if (msg->end) {
      Handle<Value> args[2] = { Null(), String::NewSymbol("end") };

      InvokeCallback(req->b->handle_, req->callback, 2, args);
      break;
    }

    Handle<Value> args[4] = {
        Null(),
        String::NewSymbol("message"),
        ValueToObject(&msg->key),
        ValueToObject(&msg->value)
    };

    InvokeCallback(req->b->handle_, req->callback, 4, args);
    delete msg;
  }

  if (msg != NULL && msg->end) {
    uv_close(reinterpret_cast<uv_handle_t*>(&req->data.range.notifier),
             BPlus::GetRangeClose);
  }
}


void BPlus::GetRangeClose(uv_handle_t* handle) {
  bp_work_req* req = container_of(handle, bp_work_req, data.range.notifier);
  req->callback.Dispose();
  req->callback.Clear();
  req->b->Unref();
  delete req->data.range.queue;
  delete req;
}


int BPlus::GetRangeFilter(void* arg, const bp_key_t* key) {
  bp_work_req* req = reinterpret_cast<bp_work_req*>(arg);

  Handle<Value> argv[1] = { ValueToObject(const_cast<bp_value_t*>(key)) };
  return !InvokeCallback(req->b->handle_, req->filter, 1, argv)->IsFalse();
}


void BPlus::GetFilteredRangeCallback(void* arg,
                                     const bp_key_t* key,
                                     const bp_value_t* value) {
  bp_work_req* req = reinterpret_cast<bp_work_req*>(arg);

  Handle<Value> argv[4] = {
      Null(),
      String::NewSymbol("message"),
      ValueToObject(const_cast<bp_key_t*>(key)),
      ValueToObject(const_cast<bp_value_t*>(value))
  };

  InvokeCallback(req->b->handle_, req->callback, 4, argv);
}


Handle<Value> BPlus::Set(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>()) ||
      !Buffer::HasInstance(args[1].As<Object>())) {
    return ThrowException(String::New("First two arguments should be Buffers"));
  }

  QUEUE_WORK(b, kSet, args[2], {
    BufferToKey(args[0].As<Object>(), &req->data.set.key);
    BufferToKey(args[1].As<Object>(), &req->data.set.value);
  })

  return Undefined();
}


Handle<Value> BPlus::Update(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>()) ||
      !Buffer::HasInstance(args[1].As<Object>())) {
    return ThrowException(String::New("First two arguments should be Buffers"));
  }

  /* initialize req manually */
  INIT_REQ(b, kUpdate, args[2])

  BufferToKey(args[0].As<Object>(), &req->data.set.key);
  BufferToKey(args[1].As<Object>(), &req->data.set.value);

  int ret;

  ret = bp_update(&b->db_,
                  &req->data.set.key,
                  &req->data.set.value,
                  BPlus::UpdateCallback,
                  reinterpret_cast<void*>(req));

  req->callback.Dispose();
  req->callback.Clear();

  delete req->data.set.key.value;
  delete req->data.set.value.value;
  delete req;

  if (ret == BP_OK) {
    return True();
  } else {
    return scope.Close(Number::New(ret));
  }
}


Handle<Value> BPlus::BulkSet(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!args[0]->IsArray()) {
    return ThrowException(String::New("First argument should be an Array"));
  }

  Local<Array> bulk = args[0].As<Array>();

  QUEUE_WORK(b, kBulkSet, args[1], {
    CopyBulkData(req, bulk->Length(), bulk);
  });

  return Undefined();
}


Handle<Value> BPlus::BulkUpdate(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!args[0]->IsArray()) {
    return ThrowException(String::New("First argument should be an Array"));
  }

  INIT_REQ(b, kBulkUpdate, args[1])
  Local<Array> bulk = args[0].As<Array>();

  CopyBulkData(req, bulk->Length(), bulk);

  int ret;

  ret = bp_bulk_update(&req->b->db_,
                       req->data.bulk.length,
                       const_cast<const bp_key_t**>(&req->data.bulk.keys),
                       const_cast<const bp_value_t**>(&req->data.bulk.values),
                       BPlus::UpdateCallback,
                       reinterpret_cast<void*>(req));

  DestroyBulkData(req);
  req->callback.Dispose();
  req->callback.Clear();
  delete req;

  if (ret == BP_OK) {
    return True();
  } else {
    return scope.Close(Number::New(ret));
  }
}


Handle<Value> BPlus::Get(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  QUEUE_WORK(b, kGet, args[1], {
    BufferToKey(args[0].As<Object>(), &req->data.get.key);
  })

  return Undefined();
}


Handle<Value> BPlus::GetPrevious(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>())) {
    return ThrowException(String::New("First argument should be a ref to val"));
  }

  QUEUE_WORK(b, kGetPrevious, args[1], {
    BufferToRawValue(args[0].As<Object>(), &req->data.previous.value);
  })

  return Undefined();
}


Handle<Value> BPlus::GetRange(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>()) ||
      !Buffer::HasInstance(args[1].As<Object>())) {
    return ThrowException(String::New("First two arguments should be Buffers"));
  }

  QUEUE_WORK(b, kGetRange, args[2], {
    BufferToKey(args[0].As<Object>(), &req->data.range.start);
    BufferToKey(args[1].As<Object>(), &req->data.range.end);
    uv_async_init(uv_default_loop(),
                  &req->data.range.notifier,
                  BPlus::GetRangeNotifier);

    req->data.range.queue = new BPQueue<BPGetRangeMessage>();
  })

  return Undefined();
}


Handle<Value> BPlus::GetFilteredRange(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>()) ||
      !Buffer::HasInstance(args[1].As<Object>())) {
    return ThrowException(String::New("First two arguments should be Buffers"));
  }

  /* initialize req manually */
  INIT_REQ(b, kGetFilteredRange, args[3])
  req->filter = Persistent<Function>::New(args[2].As<Function>());

  BufferToKey(args[0].As<Object>(), &req->data.filtered_range.start);
  BufferToKey(args[1].As<Object>(), &req->data.filtered_range.end);

  int ret;

  ret = bp_get_filtered_range(&b->db_,
                              &req->data.filtered_range.start,
                              &req->data.filtered_range.end,
                              BPlus::GetRangeFilter,
                              BPlus::GetFilteredRangeCallback,
                              reinterpret_cast<void*>(req));

  Handle<Value> argv[2];
  if (ret != BP_OK) {
    argv[0] = True();
    argv[1] = Number::New(ret);
  } else {
    argv[0] = Null();
    argv[1] = String::NewSymbol("end");
  }

  InvokeCallback(b->handle_, req->callback, 2, argv);

  req->callback.Dispose();
  req->callback.Clear();

  delete req->data.filtered_range.start.value;
  delete req->data.filtered_range.end.value;
  delete req;

  return Undefined();
}


Handle<Value> BPlus::Remove(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  QUEUE_WORK(b, kRemove, args[1], {
    BufferToKey(args[0].As<Object>(), &req->data.remove.key);
  })

  return Undefined();
}


Handle<Value> BPlus::RemoveV(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  INIT_REQ(b, kRemoveV, args[1])
  BufferToKey(args[0].As<Object>(), &req->data.remove.key);

  int ret;

  ret = bp_removev(&b->db_,
                   &req->data.remove.key,
                   BPlus::RemoveCallback,
                   reinterpret_cast<void*>(req));

  req->callback.Dispose();
  req->callback.Clear();

  delete req->data.remove.key.value;
  delete req;

  if (ret == BP_OK) {
    return True();
  } else {
    return scope.Close(Number::New(ret));
  }
}


Handle<Value> BPlus::Compact(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  QUEUE_WORK(b, kCompact, args[0], {})

  return Undefined();
}


NODE_MODULE(bplus, BPlus::Initialize);

} // namespace plus
