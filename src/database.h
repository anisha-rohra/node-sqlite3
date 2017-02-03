
#ifndef NODE_SQLITE3_SRC_DATABASE_H
#define NODE_SQLITE3_SRC_DATABASE_H


#include <string>
#include <queue>

#include <sqlite3.h>
#include <node_api_helpers.h>

#include "async.h"

namespace node_sqlite3 {

class Database;


class Database : public Napi::ObjectWrap {
public:
    static napi_persistent constructor_template;
    static NAPI_MODULE_INIT(Init);

    static inline bool HasInstance(napi_env env, napi_value val) {
        Napi::HandleScope scope;
        if (!Napi::IsObject(env, val)) return false;
        return Napi::HasInstance(env, Napi::New(env, constructor_template), val);
    }

    struct Baton {
        uv_work_t request;
        Database* db;
        napi_persistent callback;
        int status;
        std::string message;

        Baton(Database* db_, napi_value cb_) :
                db(db_), status(SQLITE_OK) {
            db->Ref();
            request.data = this;
            napi_env env = napi_get_current_env();
            callback = napi_create_persistent(env, cb_);
        }
        virtual ~Baton() {
            db->Unref();
            napi_env env = napi_get_current_env();
            napi_release_persistent(env, callback);
        }
    };

    struct OpenBaton : Baton {
        std::string filename;
        int mode;
        OpenBaton(Database* db_, napi_value cb_, const char* filename_, int mode_) :
            Baton(db_, cb_), filename(filename_), mode(mode_) {}
    };

    struct ExecBaton : Baton {
        std::string sql;
        ExecBaton(Database* db_, napi_value cb_, const char* sql_) :
            Baton(db_, cb_), sql(sql_) {}
    };

    struct LoadExtensionBaton : Baton {
        std::string filename;
        LoadExtensionBaton(Database* db_, napi_value cb_, const char* filename_) :
            Baton(db_, cb_), filename(filename_) {}
    };

    typedef void (*Work_Callback)(Baton* baton);

    struct Call {
        Call(Work_Callback cb_, Baton* baton_, bool exclusive_ = false) :
            callback(cb_), exclusive(exclusive_), baton(baton_) {};
        Work_Callback callback;
        bool exclusive;
        Baton* baton;
    };

    struct ProfileInfo {
        std::string sql;
        sqlite3_int64 nsecs;
    };

    struct UpdateInfo {
        int type;
        std::string database;
        std::string table;
        sqlite3_int64 rowid;
    };

    bool IsOpen() { return open; }
    bool IsLocked() { return locked; }

    typedef Async<std::string, Database> AsyncTrace;
    typedef Async<ProfileInfo, Database> AsyncProfile;
    typedef Async<UpdateInfo, Database> AsyncUpdate;

    friend class Statement;

protected:
    Database() : Napi::ObjectWrap(),
        _handle(NULL),
        open(false),
        locked(false),
        pending(0),
        serialize(false),
        debug_trace(NULL),
        debug_profile(NULL),
        update_event(NULL) {
    }

    ~Database() {
        RemoveCallbacks();
        sqlite3_close(_handle);
        _handle = NULL;
        open = false;
    }

    static NAPI_METHOD(New);
    static void Work_BeginOpen(Baton* baton);
    static void Work_Open(uv_work_t* req);
    static void Work_AfterOpen(uv_work_t* req);

    static NAPI_METHOD(OpenGetter);

    void Schedule(Work_Callback callback, Baton* baton, bool exclusive = false);
    void Process();

    static NAPI_METHOD(Exec);
    static void Work_BeginExec(Baton* baton);
    static void Work_Exec(uv_work_t* req);
    static void Work_AfterExec(uv_work_t* req);

    static NAPI_METHOD(Wait);
    static void Work_Wait(Baton* baton);

    static NAPI_METHOD(Close);
    static void Work_BeginClose(Baton* baton);
    static void Work_Close(uv_work_t* req);
    static void Work_AfterClose(uv_work_t* req);

    static NAPI_METHOD(LoadExtension);
    static void Work_BeginLoadExtension(Baton* baton);
    static void Work_LoadExtension(uv_work_t* req);
    static void Work_AfterLoadExtension(uv_work_t* req);

    static NAPI_METHOD(Serialize);
    static NAPI_METHOD(Parallelize);

    static NAPI_METHOD(Configure);

    static void SetBusyTimeout(Baton* baton);

    static void RegisterTraceCallback(Baton* baton);
    static void TraceCallback(void* db, const char* sql);
    static void TraceCallback(Database* db, std::string* sql);

    static void RegisterProfileCallback(Baton* baton);
    static void ProfileCallback(void* db, const char* sql, sqlite3_uint64 nsecs);
    static void ProfileCallback(Database* db, ProfileInfo* info);

    static void RegisterUpdateCallback(Baton* baton);
    static void UpdateCallback(void* db, int type, const char* database, const char* table, sqlite3_int64 rowid);
    static void UpdateCallback(Database* db, UpdateInfo* info);

    void RemoveCallbacks();

protected:
    sqlite3* _handle;

    bool open;
    bool locked;
    unsigned int pending;

    bool serialize;

    std::queue<Call*> queue;

    AsyncTrace* debug_trace;
    AsyncProfile* debug_profile;
    AsyncUpdate* update_event;
};

}

#endif
