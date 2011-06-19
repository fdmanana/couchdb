/**
 * Copyright 2011,  Filipe David Manana  <fdmanana@apache.org>
 * Web:  http://github.com/fdmanana/snappy-erlang-nif
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include <iostream>
#include <cstring>
#include <queue>

#include "erl_nif_compat.h"
#include "google-snappy/snappy.h"
#include "google-snappy/snappy-sinksource.h"

#ifdef OTP_R13B03
#error OTP R13B03 not supported. Upgrade to R13B04 or later.
#endif

#define SC_PTR(c) reinterpret_cast<char *>(c)

typedef struct {
    ErlNifEnv *env;
    ErlNifBinary data;
    ErlNifPid pid;
    ERL_NIF_TERM ref;
} task_t;

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_ERROR;

const int QUEUE_SIZE = 100; // # of elements

static ErlNifTid compThreadId;
static ErlNifTid decompThreadId;

class TaskQueue;

static TaskQueue *compQueue = NULL;
static TaskQueue *decompQueue = NULL;


static void* compressor(void*);
static void* decompressor(void*);
static ERL_NIF_TERM compress(task_t*);
static ERL_NIF_TERM decompress(task_t*);
static inline ERL_NIF_TERM make_ok(ErlNifEnv*, ERL_NIF_TERM);
static inline ERL_NIF_TERM make_ok(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM);
static inline ERL_NIF_TERM make_error(ErlNifEnv*, const char*);
static inline ERL_NIF_TERM make_error(ErlNifEnv*, ERL_NIF_TERM, const char*);
static ERL_NIF_TERM snappy_compress(ErlNifEnv*, int, const ERL_NIF_TERM[]);
static ERL_NIF_TERM snappy_decompress(ErlNifEnv*, int, const ERL_NIF_TERM[]);
static ERL_NIF_TERM snappy_uncompressed_length(ErlNifEnv*, int, const ERL_NIF_TERM[]);
static ERL_NIF_TERM snappy_is_valid(ErlNifEnv*, int, const ERL_NIF_TERM[]);
static int on_load(ErlNifEnv*, void**, ERL_NIF_TERM);
static void on_unload(ErlNifEnv*, void*);
static inline task_t* allocTask(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM);
static inline void freeTask(task_t**);


class bad_arg { };


class TaskQueue
{
public:
    TaskQueue(const unsigned int sz);
    ~TaskQueue();
    void queue(task_t*);
    task_t* dequeue();

private:
    ErlNifCond *notFull;
    ErlNifCond *notEmpty;
    ErlNifMutex *mutex;
    std::queue<task_t*> q;
    const unsigned int size;
};


TaskQueue::TaskQueue(const unsigned int sz) : size(sz)
{
    notFull = enif_cond_create(const_cast<char *>("not_full_cond"));
    if (notFull == NULL) {
        throw std::bad_alloc();
    }
    notEmpty = enif_cond_create(const_cast<char *>("not_empty_cond"));
    if (notEmpty == NULL) {
        enif_cond_destroy(notFull);
        throw std::bad_alloc();
    }
    mutex = enif_mutex_create(const_cast<char *>("queue_mutex"));
    if (mutex == NULL) {
        enif_cond_destroy(notFull);
        enif_cond_destroy(notEmpty);
        throw std::bad_alloc();
    }
}

TaskQueue::~TaskQueue()
{
    enif_cond_destroy(notFull);
    enif_cond_destroy(notEmpty);
    enif_mutex_destroy(mutex);
}

void
TaskQueue::queue(task_t *t)
{
    enif_mutex_lock(mutex);

    if (q.size() >= size) {
        enif_cond_wait(notFull, mutex);
    }
    q.push(t);
    enif_cond_signal(notEmpty);

    enif_mutex_unlock(mutex);
}

task_t*
TaskQueue::dequeue()
{
    task_t *t;

    enif_mutex_lock(mutex);

    if (q.empty()) {
        enif_cond_wait(notEmpty, mutex);
    }
    t = q.front();
    q.pop();
    enif_cond_signal(notFull);

    enif_mutex_unlock(mutex);

    return t;
}


class SnappyNifSink : public snappy::Sink
{
    public:
        SnappyNifSink(ErlNifEnv* e);
        ~SnappyNifSink();

        void Append(const char* data, size_t n);
        char* GetAppendBuffer(size_t len, char* scratch);
        ErlNifBinary& getBin();

    private:
        ErlNifEnv* env;
        ErlNifBinary bin;
        size_t length;
};

SnappyNifSink::SnappyNifSink(ErlNifEnv* e) : env(e), length(0)
{
    if(!enif_alloc_binary_compat(env, 0, &bin)) {
        env = NULL;
        throw std::bad_alloc();
    }
}

SnappyNifSink::~SnappyNifSink()
{
    if(env != NULL) {
        enif_release_binary_compat(env, &bin);
    }
}

void
SnappyNifSink::Append(const char *data, size_t n)
{
    if(data != (SC_PTR(bin.data) + length)) {
        memcpy(bin.data + length, data, n);
    }
    length += n;
}

char*
SnappyNifSink::GetAppendBuffer(size_t len, char* scratch)
{
    size_t sz;
    
    if((length + len) > bin.size) {
        sz = (len * 4) < 8192 ? 8192 : (len * 4);

        if(!enif_realloc_binary_compat(env, &bin, bin.size + sz)) {
            throw std::bad_alloc();
        }
    }

    return SC_PTR(bin.data) + length;
}

ErlNifBinary&
SnappyNifSink::getBin()
{
    if(bin.size > length) {
        if(!enif_realloc_binary_compat(env, &bin, length)) {
            throw std::bad_alloc();
        }
    }
    return bin;
}


ERL_NIF_TERM
make_ok(ErlNifEnv* env, ERL_NIF_TERM mesg)
{
    return enif_make_tuple2(env, ATOM_OK, mesg);
}


ERL_NIF_TERM
make_ok(ErlNifEnv* env, ERL_NIF_TERM ref, ERL_NIF_TERM mesg)
{
    return enif_make_tuple3(env, ATOM_OK, ref, mesg);
}


ERL_NIF_TERM
make_error(ErlNifEnv* env, const char* mesg)
{
    return enif_make_tuple2(env, ATOM_ERROR, enif_make_atom(env, mesg));
}

ERL_NIF_TERM
make_error(ErlNifEnv* env, ERL_NIF_TERM ref, const char* mesg)
{
    return enif_make_tuple3(env, ATOM_ERROR, ref, enif_make_atom(env, mesg));
}


ERL_NIF_TERM
snappy_compress(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    task_t *t = NULL;

    try {
        t = allocTask(env, argv[0], argv[1], argv[2]);
    } catch(std::bad_alloc e) {
        return make_error(env, "insufficient_memory");
    } catch(bad_arg e) {
        return enif_make_badarg(env);
    }

    compQueue->queue(t);

    return ATOM_OK;
}


ERL_NIF_TERM
compress(task_t *t)
{
    try {
        snappy::ByteArraySource source(SC_PTR(t->data.data), t->data.size);
        SnappyNifSink sink(t->env);
        snappy::Compress(&source, &sink);
        return make_ok(t->env, t->ref, enif_make_binary(t->env, &sink.getBin()));
    } catch(std::bad_alloc e) {
        return make_error(t->env, t->ref, "insufficient_memory");
    } catch(...) {
        return make_error(t->env, t->ref, "unknown");
    }
}


ERL_NIF_TERM
snappy_decompress(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    task_t *t = NULL;

    try {
        t = allocTask(env, argv[0], argv[1], argv[2]);
    } catch(std::bad_alloc e) {
        return make_error(env, "insufficient_memory");
    } catch(bad_arg e) {
        return enif_make_badarg(env);
    }

    decompQueue->queue(t);

    return ATOM_OK;
}


ERL_NIF_TERM
decompress(task_t *t)
{
    ErlNifBinary ret;
    size_t len;

    try {
        if (!snappy::GetUncompressedLength(SC_PTR(t->data.data), t->data.size, &len)) {
            return make_error(t->env, t->ref, "data_not_compressed");
        }

        if (!enif_alloc_binary_compat(t->env, len, &ret)) {
            return make_error(t->env, t->ref, "insufficient_memory");
        }

        if (!snappy::RawUncompress(SC_PTR(t->data.data), t->data.size,
                                   SC_PTR(ret.data))) {
            return make_error(t->env, t->ref, "corrupted_data");
        }

        return make_ok(t->env, t->ref, enif_make_binary(t->env, &ret));
    } catch(...) {
        return make_error(t->env, t->ref, "unknown");
    }
}


ERL_NIF_TERM
snappy_uncompressed_length(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    size_t len;

    if(!enif_inspect_iolist_as_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    try {
        if(!snappy::GetUncompressedLength(SC_PTR(bin.data), bin.size, &len)) {
            return make_error(env, "data_not_compressed");
        }
        return make_ok(env, enif_make_ulong(env, len));
    } catch(...) {
        return make_error(env, "unknown");
    }
}


ERL_NIF_TERM
snappy_is_valid(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;

    if (!enif_inspect_iolist_as_binary(env, argv[0], &bin)) {
        return enif_make_badarg(env);
    }

    try {
        if(snappy::IsValidCompressedBuffer(SC_PTR(bin.data), bin.size)) {
            return enif_make_atom(env, "true");
        } else {
            return enif_make_atom(env, "false");
        }
    } catch(...) {
        return make_error(env, "unknown");
    }
}


int
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    try {
        compQueue = new TaskQueue(QUEUE_SIZE);
        decompQueue = new TaskQueue(QUEUE_SIZE);
    } catch (std::bad_alloc e) {
        return 1;
    }


    if (0 != enif_thread_create(const_cast<char*>("compressor"),
                                &compThreadId, compressor, NULL, NULL)) {
        delete compQueue;
        delete decompQueue;
        compQueue = decompQueue = NULL;
        return 2;
    }

    if (0 != enif_thread_create(const_cast<char*>("decompressor"),
                                &decompThreadId, decompressor, NULL, NULL)) {
        delete decompQueue;
        delete decompQueue;
        compQueue = decompQueue = NULL;
        return 3;
    }

    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_ERROR = enif_make_atom(env, "error");

    return 0;
}


void
on_unload(ErlNifEnv* env, void* priv_data)
{
    void *result = NULL;

    compQueue->queue(NULL);
    decompQueue->queue(NULL);

    enif_thread_join(compThreadId, &result);
    enif_thread_join(decompThreadId, &result);

    delete compQueue;
    delete decompQueue;
    compQueue = decompQueue = NULL;
}


task_t*
allocTask(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM pid, ERL_NIF_TERM ref)
{
    task_t *t = static_cast<task_t*>(enif_alloc(sizeof(task_t)));
    ERL_NIF_TERM copy;

    if (t == NULL) {
        throw std::bad_alloc();
    }

    if (!enif_get_local_pid(env, pid, &t->pid)) {
        enif_free(t);
        throw bad_arg();
    }

    t->env = enif_alloc_env();
    if (t->env == NULL) {
        enif_free(t);
        throw std::bad_alloc();
    }

    copy = enif_make_copy(t->env, term);
    if (!enif_inspect_iolist_as_binary(t->env, copy, &t->data)) {
        enif_free_env(t->env);
        enif_free(t);
        throw bad_arg();
    }

    t->ref = enif_make_copy(t->env, ref);

    return t;
}


void
freeTask(task_t **t)
{
    enif_free_env((*t)->env);
    enif_free(*t);
    *t = NULL;
}


void*
compressor(void *arg)
{
    while (true) {
        task_t *t = compQueue->dequeue();

        if (t == NULL) {
            break;
        }

        ERL_NIF_TERM resp = compress(t);
        enif_send(NULL, &t->pid, t->env, resp);
        freeTask(&t);
    }

    return NULL;
}


void*
decompressor(void *arg)
{
    while (true) {
        task_t *t = decompQueue->dequeue();

        if (t == NULL) {
            break;
        }

        ERL_NIF_TERM resp = decompress(t);
        enif_send(NULL, &t->pid, t->env, resp);
        freeTask(&t);
    }

    return NULL;
}


static ErlNifFunc nif_functions[] = {
    {"compress", 3, snappy_compress},
    {"decompress", 3, snappy_decompress},
    {"uncompressed_length", 1, snappy_uncompressed_length},
    {"is_valid", 1, snappy_is_valid}
};


extern "C" {

ERL_NIF_INIT(snappy, nif_functions, &on_load, NULL, NULL, &on_unload);

}
