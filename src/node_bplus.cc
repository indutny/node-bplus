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
  NODE_SET_PROTOTYPE_METHOD(t, "get", BPlus::Get);
  NODE_SET_PROTOTYPE_METHOD(t, "remove", BPlus::Remove);

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
   case kGet:
    req->result = bp_get(&req->b->db_,
                         &req->data.get.key,
                         &req->data.get.value);
    free(req->data.get.key.value);
    break;
   case kRemove:
    uv_mutex_lock(&req->b->write_mutex_);
    req->result = bp_remove(&req->b->db_, &req->data.remove.key);
    uv_mutex_unlock(&req->b->write_mutex_);

    free(req->data.remove.key.value);
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
      {
        bp_value_t* v = &req->data.get.value;
        args[1] = Buffer::New(v->value, v->length)->handle_;
      }
      break;
     default:
      args[1] = Undefined();
      break;
    }
  }

  req->callback->Call(req->b->handle_, 2, args);
  req->callback.Dispose();

  req->b->Unref();
  free(work->data);
}


Handle<Value> BPlus::Set(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0]->ToObject()) ||
      !Buffer::HasInstance(args[1]->ToObject())) {
    return ThrowException(String::New("First two arguments should be Buffers"));
  }

  QUEUE_WORK(b, kSet, args[2], {
    BufferToKey(args[0]->ToObject(), &data->data.set.key);
    BufferToKey(args[1]->ToObject(), &data->data.set.value);
  })

  return Undefined();
}


Handle<Value> BPlus::Get(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0]->ToObject())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  QUEUE_WORK(b, kGet, args[1], {
    BufferToKey(args[0]->ToObject(), &data->data.get.key);
  })

  return Undefined();
}


Handle<Value> BPlus::Remove(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0]->ToObject())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  QUEUE_WORK(b, kRemove, args[1], {
    BufferToKey(args[0]->ToObject(), &data->data.remove.key);
  })

  return Undefined();
}


NODE_MODULE(bplus, BPlus::Initialize);

} // namespace plus
