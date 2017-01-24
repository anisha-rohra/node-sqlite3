#include <string.h>

#include "macros.h"
#include "database.h"
#include "statement.h"

using namespace node_sqlite3;

napi_persistent Database::constructor_template;

NAPI_MODULE_INIT(Database::Init) {
    napi_property_descriptor methods[] = {
        { "close", Close },
        { "exec", Exec },
        { "wait", Wait },
        { "loadExtension", LoadExtension },
        { "serialize", Serialize },
        { "parallelize", Parallelize },
        { "configure", Configure },
        { "open", nullptr, OpenGetter, nullptr },
    };

    napi_value ctor = napi_create_constructor(env, "Database", Database::New, nullptr, 8, methods);

    constructor_template = napi_create_persistent(env, ctor);

    Napi::Set(exports, "Database", ctor);
}

void Database::Process() {
    Napi::HandleScope scope;

    if (!open && locked && !queue.empty()) {
        EXCEPTION(Napi::New("Database handle is closed"), SQLITE_MISUSE, exception);
        napi_value argv[] = { exception };
        bool called = false;

        // Call all callbacks with the error object.
        while (!queue.empty()) {
            Call* call = queue.front();
            napi_value cb = Napi::New(call->baton->callback);
            if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
                TRY_CATCH_CALL(this->handle(), cb, 1, argv);
                called = true;
            }
            queue.pop();
            // We don't call the actual callback, so we have to make sure that
            // the baton gets destroyed.
            delete call->baton;
            delete call;
        }

        // When we couldn't call a callback function, emit an error on the
        // Database object.
        if (!called) {
            napi_value info[] = { Napi::New("error"), exception };
            EMIT_EVENT(handle(), 2, info);
        }
        return;
    }

    while (open && (!locked || pending == 0) && !queue.empty()) {
        Call* call = queue.front();

        if (call->exclusive && pending > 0) {
            break;
        }

        queue.pop();
        locked = call->exclusive;
        call->callback(call->baton);
        delete call;

        if (locked) break;
    }
}

void Database::Schedule(Work_Callback callback, Baton* baton, bool exclusive) {
    Napi::HandleScope scope;

    if (!open && locked) {
        EXCEPTION(Napi::New("Database is closed"), SQLITE_MISUSE, exception);
        napi_value cb = Napi::New(baton->callback);
        if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
            napi_value argv[] = { exception };
            TRY_CATCH_CALL(handle(), cb, 1, argv);
        }
        else {
            napi_value argv[] = { Napi::New("error"), exception };
            EMIT_EVENT(handle(), 2, argv);
        }
        return;
    }

    if (!open || ((locked || exclusive || serialize) && pending > 0)) {
        queue.push(new Call(callback, baton, exclusive || serialize));
    }
    else {
        locked = exclusive;
        callback(baton);
    }
}

NAPI_METHOD(Database::New) {
    if (!napi_is_construct_call(env, info)) {
        return Napi::ThrowTypeError("Use the new operator to create new Database objects");
    }

    GET_ARGUMENTS(3);
    REQUIRE_ARGUMENT_STRING(0, filename);
    int pos = 1;

    int mode;
    if (Napi::Length(info) >= pos && Napi::IsInt32(args[pos])) {
        mode = Napi::To<int>(args[pos++]);
    } else {
        mode = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
    }

    napi_value callback = Napi::Value();
    if (Napi::Length(info) >= pos && Napi::IsFunction(args[pos])) {
        callback = (args[pos++]);
    }

    napi_value _this = napi_get_cb_this(env, info);
    Database* db = new Database();
    db->Wrap(_this);

    Napi::Set(_this, Napi::New("filename"), args[0]);
    Napi::Set(_this, Napi::New("mode"), Napi::New(mode));

    // Start opening the database.
    OpenBaton* baton = new OpenBaton(db, callback, *filename, mode);
    Work_BeginOpen(baton);

    napi_set_return_value(env, info, _this);
}

void Database::Work_BeginOpen(Baton* baton) {
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Open, (uv_after_work_cb)Work_AfterOpen);
    assert(status == 0);
}

void Database::Work_Open(uv_work_t* req) {
    OpenBaton* baton = static_cast<OpenBaton*>(req->data);
    Database* db = baton->db;

    baton->status = sqlite3_open_v2(
        baton->filename.c_str(),
        &db->_handle,
        baton->mode,
        NULL
    );

    if (baton->status != SQLITE_OK) {
        baton->message = std::string(sqlite3_errmsg(db->_handle));
        sqlite3_close(db->_handle);
        db->_handle = NULL;
    }
    else {
        // Set default database handle values.
        sqlite3_busy_timeout(db->_handle, 1000);
    }
}

void Database::Work_AfterOpen(uv_work_t* req) {
    Napi::HandleScope scope;

    OpenBaton* baton = static_cast<OpenBaton*>(req->data);
    Database* db = baton->db;

    napi_value argv[1];
    if (baton->status != SQLITE_OK) {
        EXCEPTION(Napi::New(baton->message.c_str()), baton->status, exception);
        argv[0] = exception;
    }
    else {
        db->open = true;
        argv[0] = Napi::Null();
    }

    napi_value cb = Napi::New(baton->callback);

    if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
        TRY_CATCH_CALL(db->handle(), cb, 1, argv);
    }
    else if (!db->open) {
        napi_value info[] = { Napi::New("error"), argv[0] };
        EMIT_EVENT(db->handle(), 2, info);
    }

    if (db->open) {
        napi_value info[] = { Napi::New("open") };
        EMIT_EVENT(db->handle(), 1, info);
        db->Process();
    }

    delete baton;
}

NAPI_METHOD(Database::OpenGetter) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);
    napi_set_return_value(env, info, Napi::New(db->open));
}

NAPI_METHOD(Database::Close) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    GET_ARGUMENTS(1);
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(db, callback);
    db->Schedule(Work_BeginClose, baton, true);

    napi_set_return_value(env, info, _this);
}

void Database::Work_BeginClose(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);

    baton->db->RemoveCallbacks();
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Close, (uv_after_work_cb)Work_AfterClose);
    assert(status == 0);
}

void Database::Work_Close(uv_work_t* req) {
    Baton* baton = static_cast<Baton*>(req->data);
    Database* db = baton->db;

    baton->status = sqlite3_close(db->_handle);

    if (baton->status != SQLITE_OK) {
        baton->message = std::string(sqlite3_errmsg(db->_handle));
    }
    else {
        db->_handle = NULL;
    }
}

void Database::Work_AfterClose(uv_work_t* req) {
    Napi::HandleScope scope;

    Baton* baton = static_cast<Baton*>(req->data);
    Database* db = baton->db;

    napi_value argv[1];
    if (baton->status != SQLITE_OK) {
        EXCEPTION(Napi::New(baton->message.c_str()), baton->status, exception);
        argv[0] = exception;
    }
    else {
        db->open = false;
        // Leave db->locked to indicate that this db object has reached
        // the end of its life.
        argv[0] = Napi::Null();
    }

    napi_value cb = Napi::New(baton->callback);

    // Fire callbacks.
    if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
        TRY_CATCH_CALL(db->handle(), cb, 1, argv);
    }
    else if (db->open) {
        napi_value info[] = { Napi::New("error"), argv[0] };
        EMIT_EVENT(db->handle(), 2, info);
    }

    if (!db->open) {
        napi_value info[] = { Napi::New("close"), argv[0] };
        EMIT_EVENT(db->handle(), 1, info);
        db->Process();
    }

    delete baton;
}

NAPI_METHOD(Database::Serialize) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    GET_ARGUMENTS(1);
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    bool before = db->serialize;
    db->serialize = true;

    if (!Napi::IsEmpty(callback) && Napi::IsFunction(callback)) {
        TRY_CATCH_CALL(_this, callback, 0, NULL);
        db->serialize = before;
    }

    db->Process();

    napi_set_return_value(env, info, _this);
}

NAPI_METHOD(Database::Parallelize) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    GET_ARGUMENTS(1);
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    bool before = db->serialize;
    db->serialize = false;

    if (!Napi::IsEmpty(callback) && Napi::IsFunction(callback)) {
        TRY_CATCH_CALL(_this, callback, 0, NULL);
        db->serialize = before;
    }

    db->Process();

    napi_set_return_value(env, info, _this);
}

NAPI_METHOD(Database::Configure) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    REQUIRE_ARGUMENTS(2);
    GET_ARGUMENTS(2);

    if (Napi::Equals(args[0], Napi::New("trace"))) {
        napi_value handle = Napi::Value();
        Baton* baton = new Baton(db, handle);
        db->Schedule(RegisterTraceCallback, baton);
    }
    else if (Napi::Equals(args[0], Napi::New("profile"))) {
        napi_value handle = Napi::Value();
        Baton* baton = new Baton(db, handle);
        db->Schedule(RegisterProfileCallback, baton);
    }
    else if (Napi::Equals(args[0], Napi::New("busyTimeout"))) {
        if (!Napi::IsInt32(args[1])) {
            return Napi::ThrowTypeError("Value must be an integer");
        }
        napi_value handle = Napi::Value();
        Baton* baton = new Baton(db, handle);
        baton->status = Napi::To<int>(args[1]);
        db->Schedule(SetBusyTimeout, baton);
    }
    else {
        return Napi::ThrowError(Napi::StringConcat(
            Napi::ToString(Napi::New(args[0])),
            Napi::New(" is not a valid configuration option")
        ));
    }

    db->Process();

    napi_set_return_value(env, info, _this);
}

void Database::SetBusyTimeout(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);

    // Abuse the status field for passing the timeout.
    sqlite3_busy_timeout(baton->db->_handle, baton->status);

    delete baton;
}

void Database::RegisterTraceCallback(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    Database* db = baton->db;

    if (db->debug_trace == NULL) {
        // Add it.
        db->debug_trace = new AsyncTrace(db, TraceCallback);
        sqlite3_trace(db->_handle, TraceCallback, db);
    }
    else {
        // Remove it.
        sqlite3_trace(db->_handle, NULL, NULL);
        db->debug_trace->finish();
        db->debug_trace = NULL;
    }

    delete baton;
}

void Database::TraceCallback(void* db, const char* sql) {
    // Note: This function is called in the thread pool.
    // Note: Some queries, such as "EXPLAIN" queries, are not sent through this.
    static_cast<Database*>(db)->debug_trace->send(new std::string(sql));
}

void Database::TraceCallback(Database* db, std::string* sql) {
    // Note: This function is called in the main V8 thread.
    Napi::HandleScope scope;

    napi_value argv[] = {
        Napi::New("trace"),
        Napi::New(sql->c_str())
    };
    EMIT_EVENT(db->handle(), 2, argv);
    delete sql;
}

void Database::RegisterProfileCallback(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    Database* db = baton->db;

    if (db->debug_profile == NULL) {
        // Add it.
        db->debug_profile = new AsyncProfile(db, ProfileCallback);
        sqlite3_profile(db->_handle, ProfileCallback, db);
    }
    else {
        // Remove it.
        sqlite3_profile(db->_handle, NULL, NULL);
        db->debug_profile->finish();
        db->debug_profile = NULL;
    }

    delete baton;
}

void Database::ProfileCallback(void* db, const char* sql, sqlite3_uint64 nsecs) {
    // Note: This function is called in the thread pool.
    // Note: Some queries, such as "EXPLAIN" queries, are not sent through this.
    ProfileInfo* info = new ProfileInfo();
    info->sql = std::string(sql);
    info->nsecs = nsecs;
    static_cast<Database*>(db)->debug_profile->send(info);
}

void Database::ProfileCallback(Database *db, ProfileInfo* info) {
    Napi::HandleScope scope;

    napi_value argv[] = {
        Napi::New("profile"),
        Napi::New(info->sql.c_str()),
        Napi::New((double)info->nsecs / 1000000.0)
    };
    EMIT_EVENT(db->handle(), 3, argv);
    delete info;
}

void Database::RegisterUpdateCallback(Baton* baton) {
    assert(baton->db->open);
    assert(baton->db->_handle);
    Database* db = baton->db;

    if (db->update_event == NULL) {
        // Add it.
        db->update_event = new AsyncUpdate(db, UpdateCallback);
        sqlite3_update_hook(db->_handle, UpdateCallback, db);
    }
    else {
        // Remove it.
        sqlite3_update_hook(db->_handle, NULL, NULL);
        db->update_event->finish();
        db->update_event = NULL;
    }

    delete baton;
}

void Database::UpdateCallback(void* db, int type, const char* database,
        const char* table, sqlite3_int64 rowid) {
    // Note: This function is called in the thread pool.
    // Note: Some queries, such as "EXPLAIN" queries, are not sent through this.
    UpdateInfo* info = new UpdateInfo();
    info->type = type;
    info->database = std::string(database);
    info->table = std::string(table);
    info->rowid = rowid;
    static_cast<Database*>(db)->update_event->send(info);
}

void Database::UpdateCallback(Database *db, UpdateInfo* info) {
    Napi::HandleScope scope;

    napi_value argv[] = {
        Napi::New(sqlite_authorizer_string(info->type)),
        Napi::New(info->database.c_str()),
        Napi::New(info->table.c_str()),
        Napi::New((double)(info->rowid))
    };
    EMIT_EVENT(db->handle(), 4, argv);
    delete info;
}

NAPI_METHOD(Database::Exec) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    GET_ARGUMENTS(2);
    REQUIRE_ARGUMENT_STRING(0, sql);
    OPTIONAL_ARGUMENT_FUNCTION(1, callback);

    Baton* baton = new ExecBaton(db, callback, *sql);
    db->Schedule(Work_BeginExec, baton, true);

    napi_set_return_value(env, info, _this);
}

void Database::Work_BeginExec(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Exec, (uv_after_work_cb)Work_AfterExec);
    assert(status == 0);
}

void Database::Work_Exec(uv_work_t* req) {
    ExecBaton* baton = static_cast<ExecBaton*>(req->data);

    char* message = NULL;
    baton->status = sqlite3_exec(
        baton->db->_handle,
        baton->sql.c_str(),
        NULL,
        NULL,
        &message
    );

    if (baton->status != SQLITE_OK && message != NULL) {
        baton->message = std::string(message);
        sqlite3_free(message);
    }
}

void Database::Work_AfterExec(uv_work_t* req) {
    Napi::HandleScope scope;

    ExecBaton* baton = static_cast<ExecBaton*>(req->data);
    Database* db = baton->db;

    napi_value cb = Napi::New(baton->callback);

    if (baton->status != SQLITE_OK) {
        EXCEPTION(Napi::New(baton->message.c_str()), baton->status, exception);

        if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
            napi_value argv[] = { exception };
            TRY_CATCH_CALL(db->handle(), cb, 1, argv);
        }
        else {
            napi_value info[] = { Napi::New("error"), exception };
            EMIT_EVENT(db->handle(), 2, info);
        }
    }
    else if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
        napi_value argv[] = { Napi::Null() };
        TRY_CATCH_CALL(db->handle(), cb, 1, argv);
    }

    db->Process();

    delete baton;
}

NAPI_METHOD(Database::Wait) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    GET_ARGUMENTS(1);
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(db, callback);
    db->Schedule(Work_Wait, baton, true);

    napi_set_return_value(env, info, _this);
}

void Database::Work_Wait(Baton* baton) {
    Napi::HandleScope scope;

    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);

    napi_value cb = Napi::New(baton->callback);
    if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
        napi_value argv[] = { Napi::Null() };
        TRY_CATCH_CALL(baton->db->handle(), cb, 1, argv);
    }

    baton->db->Process();

    delete baton;
}

NAPI_METHOD(Database::LoadExtension) {
    napi_value _this = napi_get_cb_this(env, info);
    Database* db = Napi::ObjectWrap::Unwrap<Database>(_this);

    GET_ARGUMENTS(2);
    REQUIRE_ARGUMENT_STRING(0, filename);
    OPTIONAL_ARGUMENT_FUNCTION(1, callback);

    Baton* baton = new LoadExtensionBaton(db, callback, *filename);
    db->Schedule(Work_BeginLoadExtension, baton, true);

    napi_set_return_value(env, info, _this);
}

void Database::Work_BeginLoadExtension(Baton* baton) {
    assert(baton->db->locked);
    assert(baton->db->open);
    assert(baton->db->_handle);
    assert(baton->db->pending == 0);
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_LoadExtension, reinterpret_cast<uv_after_work_cb>(Work_AfterLoadExtension));
    assert(status == 0);
}

void Database::Work_LoadExtension(uv_work_t* req) {
    LoadExtensionBaton* baton = static_cast<LoadExtensionBaton*>(req->data);

    sqlite3_enable_load_extension(baton->db->_handle, 1);

    char* message = NULL;
    baton->status = sqlite3_load_extension(
        baton->db->_handle,
        baton->filename.c_str(),
        0,
        &message
    );

    sqlite3_enable_load_extension(baton->db->_handle, 0);

    if (baton->status != SQLITE_OK && message != NULL) {
        baton->message = std::string(message);
        sqlite3_free(message);
    }
}

void Database::Work_AfterLoadExtension(uv_work_t* req) {
    Napi::HandleScope scope;

    LoadExtensionBaton* baton = static_cast<LoadExtensionBaton*>(req->data);
    Database* db = baton->db;
    napi_value cb = Napi::New(baton->callback);

    if (baton->status != SQLITE_OK) {
        EXCEPTION(Napi::New(baton->message.c_str()), baton->status, exception);

        if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
            napi_value argv[] = { exception };
            TRY_CATCH_CALL(db->handle(), cb, 1, argv);
        }
        else {
            napi_value info[] = { Napi::New("error"), exception };
            EMIT_EVENT(db->handle(), 2, info);
        }
    }
    else if (!Napi::IsEmpty(cb) && Napi::IsFunction(cb)) {
        napi_value argv[] = { Napi::Null() };
        TRY_CATCH_CALL(db->handle(), cb, 1, argv);
    }

    db->Process();

    delete baton;
}

void Database::RemoveCallbacks() {
    if (debug_trace) {
        debug_trace->finish();
        debug_trace = NULL;
    }
    if (debug_profile) {
        debug_profile->finish();
        debug_profile = NULL;
    }
}
