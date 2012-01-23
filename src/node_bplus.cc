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

#define QUEUE_ACTION(b, callback)\
    uv_work_t* work = new uv_work_t();\
    bplus_work_data* data = new bplus_work_data();\
    data->b = b;\
    data->result = BP_OK;\
    data->callback = callback;\
    work->data = reinterpret_cast<void*>(data);\
    b->Ref();

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


void BPlus::DoSet(uv_work_t* work) {
  bp_base_req* req = container_of(work, bp_base_req, w);
  uv_mutex_lock(&req->b->write_mutex_);

  req->result = bp_set(&req->b->db_, &req->key, &req->value);

  uv_mutex_unlock(&req->b->write_mutex_);
}


void BPlus::AfterSet(uv_work_t* work) {
  HandleScope scope;

  bp_base_req* req = container_of(work, bp_base_req, w);

  Handle<Value> args[1] = { req->result == BP_OK ? False() : True() };
  req->callback->Call(req->b->handle_, 1, args);

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

  Local<Function> fn = args[2].As<Function>();
  Persistent<Function> callback = Persistent<Function>::New(fn);

  bp_base_req* data = new bp_base_req;
  data->b = b;
  data->callback = callback;
  data->w.data = (void*) data;
  b->Ref();

  BufferToKey(args[0]->ToObject(), &data->key);
  BufferToKey(args[1]->ToObject(), &data->value);

  uv_queue_work(uv_default_loop(), &data->w, BPlus::DoSet, BPlus::AfterSet);

  return Undefined();
}


void BPlus::DoGet(uv_work_t* work) {
  bp_base_req* req = container_of(work, bp_base_req, w);

  req->result = bp_get(&req->b->db_, &req->key, &req->value);
}


void BPlus::AfterGet(uv_work_t* work) {
  HandleScope scope;

  bp_base_req* req = container_of(work, bp_base_req, w);

  if (req->result == BP_OK) {
    Handle<Value> args[2] = {
        False(),
        Buffer::New(req->value.value, req->value.length)->handle_
    };
    free(req->value.value);
    req->callback->Call(req->b->handle_, 2, args);
  } else {
    Handle<Value> args[1] = { True() };
    req->callback->Call(req->b->handle_, 1, args);
  }

  req->b->Unref();
  free(work->data);
}


Handle<Value> BPlus::Get(const Arguments &args) {
  HandleScope scope;

  UNWRAP
  CHECK_OPENED(b)

  if (!Buffer::HasInstance(args[0]->ToObject())) {
    return ThrowException(String::New("First argument should be Buffer"));
  }

  Local<Function> fn = args[1].As<Function>();
  Persistent<Function> callback = Persistent<Function>::New(fn);

  bp_base_req* data = new bp_base_req;
  data->b = b;
  data->callback = callback;
  data->w.data = (void*) data;
  b->Ref();

  BufferToKey(args[0]->ToObject(), &data->key);

  uv_queue_work(uv_default_loop(), &data->w, BPlus::DoGet, BPlus::AfterGet);

  return Undefined();
}


NODE_MODULE(bplus, BPlus::Initialize);

} // namespace plus
