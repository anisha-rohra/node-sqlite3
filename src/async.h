#ifndef NODE_SQLITE3_SRC_ASYNC_H
#define NODE_SQLITE3_SRC_ASYNC_H

#include "threading.h"
#include <node_version.h>
#include <assert.h>
#include <napi.h>

#if defined(NODE_SQLITE3_BOOST_THREADING)
#include <boost/thread/mutex.hpp>
#endif

// Generic uv_async handler.
template <class Item, class Parent> class Async: public Napi::AsyncWorker {
    typedef void (*Callback)(Parent* parent, Item* item);

protected:
    NODE_SQLITE3_MUTEX_t
    std::vector<Item*> data;
    Callback callback;
public:
    Parent* parent;

public:
    Async(Napi::Function& callback_, Parent* parent_)
    : AsyncWorker(callback_), parent(parent_) {
        NODE_SQLITE3_MUTEX_INIT
    }

    void add(Item* item) {
        NODE_SQLITE3_MUTEX_LOCK(&mutex);
        data.push_back(item);
        NODE_SQLITE3_MUTEX_UNLOCK(&mutex)
    }

    void send() {
        Queue();
    }

    void send(Item* item) {
        add(item);
        send();
    }

    void Execute() {
        std::vector<Item*> rows;
        NODE_SQLITE3_MUTEX_LOCK(&mutex)
        rows.swap(data);
        NODE_SQLITE3_MUTEX_UNLOCK(&mutex)
        for (unsigned int i = 0, size = rows.size(); i < size; i++) {
            callback(parent, rows[i]);
        }
    }

    ~Async() {
        NODE_SQLITE3_MUTEX_DESTROY
    }
};

#endif
