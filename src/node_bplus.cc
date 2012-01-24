#include "node_bplus.h"

#include <uv.h>
#include <node.h>
#include <node_object_wrap.h>
#include <node_buffer.h>
#include <v8.h>

#include <bplus.h>

#define UNWRAP\
    BPlus *b = ObjectWrap::Unwrap<BPlus>(args.Holder());

#define CHECK_OPENED(b)\
    if (!b->opened_) {\
      return ThrowException(String::New("Database wasn't opened"));\
    }

#define QUEUE_WORK(b_, type_, callback_, prepare_)\
      Local<Function> fn = callback_.As<Function>();\
      Persistent<Function> callback = Persistent<Function>::New(fn);\
      bp_work_req* data = new bp_work_req;\
      data->b = b_;\
      data->type = type_;\
      data->callback = callback;\
      data->w.data = (void*) data;\
      b->Ref();\
      prepare_\
      uv_queue_work(uv_default_loop(),\
                    &data->w,\
                    BPlus::DoWork,\
                    BPlus::AfterWork);

namespace bplus {

using namespace node;
using namespace v8;


void BPlus::Initialize(Handle<Object> target) {
  Local<FunctionTemplate> t = FunctionTemplate::New(BPlus::New);

  t->InstanceTemplate()->SetInternalFieldCount(1);
  t->SetClassName(String::NewSymbol("BPlus"));

  NODE_SET_PROTOTYPE_METHOD(t, "open", BPlus::Open);
  NODE_SET_PROTOTYPE_METHOD(t, "close", BPlus::Close);

  NODE_SET_PROTOTYPE_METHOD(t, "set", BPlus::Set);
  NODE_SET_PROTOTYPE_METHOD(t, "bulkSet", BPlus::BulkSet);
  NODE_SET_PROTOTYPE_METHOD(t, "get", BPlus::Get);
  NODE_SET_PROTOTYPE_METHOD(t, "remove", BPlus::Remove);
  NODE_SET_PROTOTYPE_METHOD(t, "compact", BPlus::Compact);
  NODE_SET_PROTOTYPE_METHOD(t, "getPrevious", BPlus::GetPrevious);
  NODE_SET_PROTOTYPE_METHOD(t, "getRange", BPlus::GetRange);

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
    uv_mutex_lock(&req->b->write_mutex_);
    req->result = bp_set(&req->b->db_,
                         &req->data.set.key,
                         &req->data.set.value);
    uv_mutex_unlock(&req->b->write_mutex_);

    free(req->data.set.key.value);
    free(req->data.set.value.value);
    break;
   case kBulkSet:
    uv_mutex_lock(&req->b->write_mutex_);
    req->result = bp_bulk_set(&req->b->db_,
                              req->data.bulk.length,
                              (const bp_key_t**) &req->data.bulk.keys,
                              (const bp_value_t**) &req->data.bulk.values);
    uv_mutex_unlock(&req->b->write_mutex_);

    for (uint64_t i = 0; i < req->data.bulk.length; i++) {
      free(req->data.bulk.keys[i].value);
      free(req->data.bulk.values[i].value);
    }

    free(req->data.bulk.keys);
    free(req->data.bulk.values);
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
                               (void*) req);
    break;
   case kRemove:
    uv_mutex_lock(&req->b->write_mutex_);
    req->result = bp_remove(&req->b->db_, &req->data.remove.key);
    uv_mutex_unlock(&req->b->write_mutex_);

    free(req->data.remove.key.value);
    break;
   case kCompact:
    uv_mutex_lock(&req->b->write_mutex_);
    req->result = bp_compact(&req->b->db_);
    uv_mutex_unlock(&req->b->write_mutex_);
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

  req->callback->Call(req->b->handle_, 2, args);
  req->callback.Dispose();
  req->b->Unref();

  delete req;
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
      req->callback->Call(req->b->handle_, 2, args);
      break;
    }

    Handle<Value> args[4] = {
        Null(),
        String::NewSymbol("message"),
        ValueToObject(&msg->key),
        ValueToObject(&msg->value)
    };
    req->callback->Call(req->b->handle_, 4, args);
    delete msg;
  }

  if (msg != NULL && msg->end) {
    uv_close((uv_handle_t*) &req->data.range.notifier, BPlus::GetRangeClose);
  }
}


void BPlus::GetRangeClose(uv_handle_t* handle) {
  bp_work_req* req = container_of(handle, bp_work_req, data.range.notifier);
  req->callback.Dispose();
  req->b->Unref();
  delete req->data.range.queue;
  delete req;
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
    BufferToKey(args[0].As<Object>(), &data->data.set.key);
    BufferToKey(args[1].As<Object>(), &data->data.set.value);
  })

  return Undefined();
}


Handle<Value> BPlus::BulkSet(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!args[0]->IsArray()) {
    return ThrowException(String::New("First argument should be an Array"));
  }

  Local<Array> bulk = args[0].As<Array>();
  uint32_t len = bulk->Length();

  QUEUE_WORK(b, kBulkSet, args[1], {
    data->data.bulk.length = len;
    data->data.bulk.keys = new bp_key_t[len];
    data->data.bulk.values = new bp_key_t[len];

    for (uint32_t i = 0; i < len; i++) {
      Local<Object> item = bulk->Get(i).As<Object>();
      BufferToKey(item->Get(String::NewSymbol("key")).As<Object>(),
                  &data->data.bulk.keys[i]);
      BufferToKey(item->Get(String::NewSymbol("value")).As<Object>(),
                  &data->data.bulk.values[i]);
    }
  });

  return Undefined();
}


Handle<Value> BPlus::Get(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0].As<Object>())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  QUEUE_WORK(b, kGet, args[1], {
    BufferToKey(args[0].As<Object>(), &data->data.get.key);
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
    BufferToRawValue(args[0].As<Object>(), &data->data.previous.value);
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
    BufferToKey(args[0].As<Object>(), &data->data.range.start);
    BufferToKey(args[1].As<Object>(), &data->data.range.end);
    uv_async_init(uv_default_loop(),
                  &data->data.range.notifier,
                  BPlus::GetRangeNotifier);

    data->data.range.queue = new BPQueue<BPGetRangeMessage>();
  })

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
    BufferToKey(args[0].As<Object>(), &data->data.remove.key);
  })

  return Undefined();
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
