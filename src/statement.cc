#include <string.h>
#include <node_api_helpers.h>

#include "macros.h"
#include "database.h"
#include "statement.h"

using namespace node_sqlite3;

napi_persistent Statement::constructor_template;

NAPI_MODULE_INIT(Statement::Init) {
    napi_property_descriptor methods [] = {
        { "bind", Bind },
        { "get", Get},
        { "run", Run },
        { "all", All },
        { "each", Each },
        { "reset", Reset },
        { "finalize", Finalize }
    };

    napi_value ctor = napi_create_constructor(env, "Statement", New, nullptr, 7, methods);

    constructor_template = napi_create_persistent(env, ctor);

    Napi::Set(env, exports, "Statement", ctor);
}

void Statement::Process() {
    if (finalized && !queue.empty()) {
        return CleanQueue();
    }

    while (prepared && !locked && !queue.empty()) {
        Call* call = queue.front();
        queue.pop();

        call->callback(call->baton);
        delete call;
    }
}

void Statement::Schedule(Work_Callback callback, Baton* baton) {
    if (finalized) {
        queue.push(new Call(callback, baton));
        CleanQueue();
    }
    else if (!prepared || locked) {
        queue.push(new Call(callback, baton));
    }
    else {
        callback(baton);
    }
}

template <class T> void Statement::Error(T* baton) {
    Napi::HandleScope scope;
    napi_env env = napi_get_current_env();

    Statement* stmt = baton->stmt;
    // Fail hard on logic errors.
    assert(stmt->status != 0);
    EXCEPTION(Napi::New(env, stmt->message.c_str()), stmt->status, exception);

    napi_value cb = Napi::New(env, baton->callback);

    if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
        napi_value argv[] = { exception };
        TRY_CATCH_CALL(stmt->handle(), cb, 1, argv);
    }
    else {
        napi_value argv[] = { Napi::New(env, "error"), exception };
        EMIT_EVENT(stmt->handle(), 2, argv);
    }
}

// { Database db, String sql, Array params, Function callback }
NAPI_METHOD(Statement::New) {
    if (!napi_is_construct_call(env, info)) {
        return Napi::ThrowTypeError(env, "Use the new operator to create new Statement objects");
    }

    GET_ARGUMENTS(3);
    int length = Napi::Length(env, info);
    if (length <= 0 || !Database::HasInstance(env, args[0])) {
        return Napi::ThrowTypeError(env, "Database object expected");
    }
    else if (length <= 1 || !Napi::IsString(env, args[1])) {
        return Napi::ThrowTypeError(env, "SQL query expected");
    }
    else if (length > 2 && !Napi::IsUndefined(env, args[2]) && !Napi::IsFunction(env, args[2])) {
        return Napi::ThrowTypeError(env, "Callback expected");
    }

    Database* db = Napi::ObjectWrap::Unwrap<Database>(args[0]);
    napi_value sql = args[1];

    napi_value _this = napi_get_cb_this(env, info);
    Napi::Set(env, _this, Napi::New(env, "sql"), sql);

    Statement* stmt = new Statement(db);
    stmt->Wrap(_this);

    PrepareBaton* baton = new PrepareBaton(db, (args[2]), stmt);
    baton->sql = std::string(*Napi::Utf8String(sql));
    db->Schedule(Work_BeginPrepare, baton);

    napi_set_return_value(env, info, _this);
}

void Statement::Work_BeginPrepare(Database::Baton* baton) {
    assert(baton->db->open);
    baton->db->pending++;
    int status = uv_queue_work(uv_default_loop(),
        &baton->request, Work_Prepare, (uv_after_work_cb)Work_AfterPrepare);
    assert(status == 0);
}

void Statement::Work_Prepare(uv_work_t* req) {
    STATEMENT_INIT(PrepareBaton);

    // In case preparing fails, we use a mutex to make sure we get the associated
    // error message.
    sqlite3_mutex* mtx = sqlite3_db_mutex(baton->db->_handle);
    sqlite3_mutex_enter(mtx);

    stmt->status = sqlite3_prepare_v2(
        baton->db->_handle,
        baton->sql.c_str(),
        baton->sql.size(),
        &stmt->_handle,
        NULL
    );

    if (stmt->status != SQLITE_OK) {
        stmt->message = std::string(sqlite3_errmsg(baton->db->_handle));
        stmt->_handle = NULL;
    }

    sqlite3_mutex_leave(mtx);
}

void Statement::Work_AfterPrepare(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(PrepareBaton);

    if (stmt->status != SQLITE_OK) {
        Error(baton);
        stmt->Finalize();
    }
    else {
        stmt->prepared = true;
        napi_value cb = Napi::New(env, baton->callback);
        if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
            napi_value argv[] = { Napi::Null(env) };
            TRY_CATCH_CALL(stmt->handle(), cb, 1, argv);
        }
    }

    STATEMENT_END();
}

template <class T> Values::Field*
                   Statement::BindParameter(napi_env env, const napi_value source, T pos) {
    if (Napi::IsString(env, source) || Napi::IsRegExp(env, source)) {
        Napi::Utf8String val(source);
        return new Values::Text(pos, val.length(), *val);
    }
    else if (Napi::IsNumber(env, source)) {
        return new Values::Integer(pos, Napi::To<int32_t>(env, source));
    }
    else if (Napi::IsNumber(env, source)) {
        return new Values::Float(pos, Napi::To<double>(env, source));
    }
    else if (Napi::IsBoolean(env, source)) {
        return new Values::Integer(pos, Napi::To<bool>(env, source) ? 1 : 0);
    }
    else if (Napi::IsNull(env, source)) {
        return new Values::Null(pos);
    }
    else if (napi_buffer_has_instance(env, source)) {
        napi_value buffer = Napi::ToObject(env, source);
        return new Values::Blob(pos, napi_buffer_length(env, buffer), napi_buffer_data(env, buffer));
    }
    else if (Napi::IsDate(env, source)) {
        return new Values::Float(pos, Napi::To<double>(env, source));
    }
    else {
        return NULL;
    }
}

template <class T> T* Statement::Bind(napi_env env, napi_callback_info info, int start, int last) {
    Napi::HandleScope scope;

    int len = Napi::Length(env, info);
    napi_value* args = new napi_value[len];
    napi_get_cb_args(env, info, args, len);

    if (last < 0) last = len;
    napi_value callback = Napi::New(env);
    if (last > start && Napi::IsFunction(env, args[last - 1])) {
        callback = (args[last - 1]);
        last--;
    }

    T* baton = new T(this, callback);

    if (start < last) {
        if (Napi::IsArray(env, args[start])) {
            napi_value array = args[start];
            int length = Napi::Length(env, array);
            // Note: bind parameters start with 1.
            for (int i = 0, pos = 1; i < length; i++, pos++) {
                baton->parameters.push_back(BindParameter(env, Napi::Get(env, array, i), pos));
            }
        }
        else if (!Napi::IsObject(env, args[start]) || Napi::IsRegExp(env, args[start]) || Napi::IsDate(env, args[start]) || napi_buffer_has_instance(env, args[start])) {
            // Parameters directly in array.
            // Note: bind parameters start with 1.
            for (int i = start, pos = 1; i < last; i++, pos++) {
                baton->parameters.push_back(BindParameter(env, args[i], pos));
            }
        }
        else if (Napi::IsObject(env, args[start])) {
            napi_value object = args[start];
            napi_value array = Napi::GetPropertyNames(env, object);
            int length = Napi::Length(env, array);
            for (int i = 0; i < length; i++) {
                napi_value name = Napi::Get(env, array, i);

                if (Napi::IsNumber(env, name)) {
                    baton->parameters.push_back(
                        BindParameter(env, Napi::Get(env, object, name), Napi::To<int32_t>(env, name)));
                }
                else {
                    baton->parameters.push_back(BindParameter(env, Napi::Get(env, object, name),
                        *Napi::Utf8String(name)));
                }
            }
        }
        else {
            delete args;
            return NULL;
        }
    }
    delete args;

    return baton;
}

bool Statement::Bind(const Parameters & parameters) {
    if (parameters.size() == 0) {
        return true;
    }

    sqlite3_reset(_handle);
    sqlite3_clear_bindings(_handle);

    Parameters::const_iterator it = parameters.begin();
    Parameters::const_iterator end = parameters.end();

    for (; it < end; ++it) {
        Values::Field* field = *it;

        if (field != NULL) {
            int pos;
            if (field->index > 0) {
                pos = field->index;
            }
            else {
                pos = sqlite3_bind_parameter_index(_handle, field->name.c_str());
            }

            switch (field->type) {
                case SQLITE_INTEGER: {
                    status = sqlite3_bind_int(_handle, pos,
                        ((Values::Integer*)field)->value);
                } break;
                case SQLITE_FLOAT: {
                    status = sqlite3_bind_double(_handle, pos,
                        ((Values::Float*)field)->value);
                } break;
                case SQLITE_TEXT: {
                    status = sqlite3_bind_text(_handle, pos,
                        ((Values::Text*)field)->value.c_str(),
                        ((Values::Text*)field)->value.size(), SQLITE_TRANSIENT);
                } break;
                case SQLITE_BLOB: {
                    status = sqlite3_bind_blob(_handle, pos,
                        ((Values::Blob*)field)->value,
                        ((Values::Blob*)field)->length, SQLITE_TRANSIENT);
                } break;
                case SQLITE_NULL: {
                    status = sqlite3_bind_null(_handle, pos);
                } break;
            }

            if (status != SQLITE_OK) {
                message = std::string(sqlite3_errmsg(db->_handle));
                return false;
            }
        }
    }

    return true;
}

NAPI_METHOD(Statement::Bind) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    Baton* baton = stmt->Bind<Baton>(env, info);
    if (baton == NULL) {
        return Napi::ThrowTypeError(env, "Data type is not supported");
    }
    else {
        stmt->Schedule(Work_BeginBind, baton);
        napi_set_return_value(env, info, _this);
    }
}

void Statement::Work_BeginBind(Baton* baton) {
    STATEMENT_BEGIN(Bind);
}

void Statement::Work_Bind(uv_work_t* req) {
    STATEMENT_INIT(Baton);

    sqlite3_mutex* mtx = sqlite3_db_mutex(stmt->db->_handle);
    sqlite3_mutex_enter(mtx);
    stmt->Bind(baton->parameters);
    sqlite3_mutex_leave(mtx);
}

void Statement::Work_AfterBind(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(Baton);

    if (stmt->status != SQLITE_OK) {
        Error(baton);
    }
    else {
        // Fire callbacks.
        napi_value cb = Napi::New(env, baton->callback);
        if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
            napi_value argv[] = { Napi::Null(env) };
            TRY_CATCH_CALL(stmt->handle(), cb, 1, argv);
        }
    }

    STATEMENT_END();
}



NAPI_METHOD(Statement::Get) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    Baton* baton = stmt->Bind<RowBaton>(env, info);
    if (baton == NULL) {
        return Napi::ThrowError(env, "Data type is not supported");
    }
    else {
        stmt->Schedule(Work_BeginGet, baton);
        napi_set_return_value(env, info, _this);
    }
}

void Statement::Work_BeginGet(Baton* baton) {
    STATEMENT_BEGIN(Get);
}

void Statement::Work_Get(uv_work_t* req) {
    STATEMENT_INIT(RowBaton);

    if (stmt->status != SQLITE_DONE || baton->parameters.size()) {
        sqlite3_mutex* mtx = sqlite3_db_mutex(stmt->db->_handle);
        sqlite3_mutex_enter(mtx);

        if (stmt->Bind(baton->parameters)) {
            stmt->status = sqlite3_step(stmt->_handle);

            if (!(stmt->status == SQLITE_ROW || stmt->status == SQLITE_DONE)) {
                stmt->message = std::string(sqlite3_errmsg(stmt->db->_handle));
            }
        }

        sqlite3_mutex_leave(mtx);

        if (stmt->status == SQLITE_ROW) {
            // Acquire one result row before returning.
            GetRow(&baton->row, stmt->_handle);
        }
    }
}

void Statement::Work_AfterGet(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(RowBaton);

    if (stmt->status != SQLITE_ROW && stmt->status != SQLITE_DONE) {
        Error(baton);
    }
    else {
        // Fire callbacks.
        napi_value cb = Napi::New(env, baton->callback);
        if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
            if (stmt->status == SQLITE_ROW) {
                // Create the result array from the data we acquired.
                napi_value argv[] = { Napi::Null(env), RowToJS(&baton->row) };
                TRY_CATCH_CALL(stmt->handle(), cb, 2, argv);
            }
            else {
                napi_value argv[] = { Napi::Null(env) };
                TRY_CATCH_CALL(stmt->handle(), cb, 1, argv);
            }
        }
    }

    STATEMENT_END();
}

NAPI_METHOD(Statement::Run) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    Baton* baton = stmt->Bind<RunBaton>(env, info);
    if (baton == NULL) {
        return Napi::ThrowError(env, "Data type is not supported");
    }
    else {
        stmt->Schedule(Work_BeginRun, baton);
        napi_set_return_value(env, info, _this);
    }
}

void Statement::Work_BeginRun(Baton* baton) {
    STATEMENT_BEGIN(Run);
}

void Statement::Work_Run(uv_work_t* req) {
    STATEMENT_INIT(RunBaton);

    sqlite3_mutex* mtx = sqlite3_db_mutex(stmt->db->_handle);
    sqlite3_mutex_enter(mtx);

    // Make sure that we also reset when there are no parameters.
    if (!baton->parameters.size()) {
        sqlite3_reset(stmt->_handle);
    }

    if (stmt->Bind(baton->parameters)) {
        stmt->status = sqlite3_step(stmt->_handle);

        if (!(stmt->status == SQLITE_ROW || stmt->status == SQLITE_DONE)) {
            stmt->message = std::string(sqlite3_errmsg(stmt->db->_handle));
        }
        else {
            baton->inserted_id = sqlite3_last_insert_rowid(stmt->db->_handle);
            baton->changes = sqlite3_changes(stmt->db->_handle);
        }
    }

    sqlite3_mutex_leave(mtx);
}

void Statement::Work_AfterRun(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(RunBaton);

    if (stmt->status != SQLITE_ROW && stmt->status != SQLITE_DONE) {
        Error(baton);
    }
    else {
        // Fire callbacks.
        napi_value cb = Napi::New(env, baton->callback);
        if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
            Napi::Set(env, stmt->handle(), Napi::New(env, "lastID"), Napi::New(env, (double)(baton->inserted_id)));
            Napi::Set(env, stmt->handle(), Napi::New(env, "changes"), Napi::New(env, baton->changes));

            napi_value argv[] = { Napi::Null(env) };
            TRY_CATCH_CALL(stmt->handle(), cb, 1, argv);
        }
    }

    STATEMENT_END();
}

NAPI_METHOD(Statement::All) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    Baton* baton = stmt->Bind<RowsBaton>(env, info);
    if (baton == NULL) {
        return Napi::ThrowError(env, "Data type is not supported");
    }
    else {
        stmt->Schedule(Work_BeginAll, baton);
        napi_set_return_value(env, info, _this);
    }
}

void Statement::Work_BeginAll(Baton* baton) {
    STATEMENT_BEGIN(All);
}

void Statement::Work_All(uv_work_t* req) {
    STATEMENT_INIT(RowsBaton);

    sqlite3_mutex* mtx = sqlite3_db_mutex(stmt->db->_handle);
    sqlite3_mutex_enter(mtx);

    // Make sure that we also reset when there are no parameters.
    if (!baton->parameters.size()) {
        sqlite3_reset(stmt->_handle);
    }

    if (stmt->Bind(baton->parameters)) {
        while ((stmt->status = sqlite3_step(stmt->_handle)) == SQLITE_ROW) {
            Row* row = new Row();
            GetRow(row, stmt->_handle);
            baton->rows.push_back(row);
        }

        if (stmt->status != SQLITE_DONE) {
            stmt->message = std::string(sqlite3_errmsg(stmt->db->_handle));
        }
    }

    sqlite3_mutex_leave(mtx);
}

void Statement::Work_AfterAll(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(RowsBaton);

    if (stmt->status != SQLITE_DONE) {
        Error(baton);
    }
    else {
        // Fire callbacks.
        napi_value cb = Napi::New(env, baton->callback);
        if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
            if (baton->rows.size()) {
                // Create the result array from the data we acquired.
                napi_value result = Napi::NewArray(env, baton->rows.size());
                Rows::const_iterator it = baton->rows.begin();
                Rows::const_iterator end = baton->rows.end();
                for (int i = 0; it < end; ++it, i++) {
                    Napi::Set(env, result, i, RowToJS(*it));
                    delete *it;
                }

                napi_value argv[] = { Napi::Null(env), result };
                TRY_CATCH_CALL(stmt->handle(), cb, 2, argv);
            }
            else {
                // There were no result rows.
                napi_value argv[] = {
                    Napi::Null(env),
                    Napi::NewArray(env, 0)
                };
                TRY_CATCH_CALL(stmt->handle(), cb, 2, argv);
            }
        }
    }

    STATEMENT_END();
}

NAPI_METHOD(Statement::Each) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    napi_value* args;
    int last = Napi::Length(env, info);
    args = (napi_value*) malloc(last * sizeof(napi_value));
    napi_get_cb_args(env, info, args, last);

    napi_value completed = Napi::New(env);
    if (last >= 2 && Napi::IsFunction(env, args[last - 1]) &&
        Napi::IsFunction(env, args[last - 2])) {
        completed = (args[--last]);
    }

    EachBaton* baton = stmt->Bind<EachBaton>(env, info, 0, last);
    if (baton == NULL) {
        return Napi::ThrowError(env, "Data type is not supported");
    }
    else {
        baton->completed = napi_create_persistent(env, completed);
        stmt->Schedule(Work_BeginEach, baton);
        napi_set_return_value(env, info, _this);
    }
    delete args;
}

void Statement::Work_BeginEach(Baton* baton) {
    // Only create the Async object when we're actually going into
    // the event loop. This prevents dangling events.
    napi_env env = napi_get_current_env();
    EachBaton* each_baton = static_cast<EachBaton*>(baton);
    each_baton->async = new Async(each_baton->stmt, reinterpret_cast<uv_async_cb>(AsyncEach));
    each_baton->async->item_cb = napi_create_persistent(env, napi_get_persistent_value(env, each_baton->callback));
    each_baton->async->completed_cb = napi_create_persistent(env, napi_get_persistent_value(env, each_baton->completed));

    STATEMENT_BEGIN(Each);
}

void Statement::Work_Each(uv_work_t* req) {
    STATEMENT_INIT(EachBaton);

    Async* async = baton->async;

    sqlite3_mutex* mtx = sqlite3_db_mutex(stmt->db->_handle);

    int retrieved = 0;

    // Make sure that we also reset when there are no parameters.
    if (!baton->parameters.size()) {
        sqlite3_reset(stmt->_handle);
    }

    if (stmt->Bind(baton->parameters)) {
        while (true) {
            sqlite3_mutex_enter(mtx);
            stmt->status = sqlite3_step(stmt->_handle);
            if (stmt->status == SQLITE_ROW) {
                sqlite3_mutex_leave(mtx);
                Row* row = new Row();
                GetRow(row, stmt->_handle);
                NODE_SQLITE3_MUTEX_LOCK(&async->mutex)
                async->data.push_back(row);
                retrieved++;
                NODE_SQLITE3_MUTEX_UNLOCK(&async->mutex)

                uv_async_send(&async->watcher);
            }
            else {
                if (stmt->status != SQLITE_DONE) {
                    stmt->message = std::string(sqlite3_errmsg(stmt->db->_handle));
                }
                sqlite3_mutex_leave(mtx);
                break;
            }
        }
    }

    async->completed = true;
    uv_async_send(&async->watcher);
}

void Statement::CloseCallback(uv_handle_t* handle) {
    assert(handle != NULL);
    assert(handle->data != NULL);
    Async* async = static_cast<Async*>(handle->data);
    delete async;
}

void Statement::AsyncEach(uv_async_t* handle, int status) {
    Napi::HandleScope scope;
    napi_env env = napi_get_current_env();

    Async* async = static_cast<Async*>(handle->data);

    while (true) {
        // Get the contents out of the data cache for us to process in the JS callback.
        Rows rows;
        NODE_SQLITE3_MUTEX_LOCK(&async->mutex)
        rows.swap(async->data);
        NODE_SQLITE3_MUTEX_UNLOCK(&async->mutex)

        if (rows.empty()) {
            break;
        }

        napi_value cb = Napi::New(env, async->item_cb);
        if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
            napi_value argv[2];
            argv[0] = Napi::Null(env);

            Rows::const_iterator it = rows.begin();
            Rows::const_iterator end = rows.end();
            for (int i = 0; it < end; ++it, i++) {
                argv[1] = RowToJS(*it);
                async->retrieved++;
                TRY_CATCH_CALL(async->stmt->handle(), cb, 2, argv);
                delete *it;
            }
        }
    }

    napi_value cb = Napi::New(env, async->completed_cb);
    if (async->completed) {
        if (!Napi::IsEmpty(env, cb) &&
                Napi::IsFunction(env, cb)) {
            napi_value argv[] = {
                Napi::Null(env),
                Napi::New(env, async->retrieved)
            };
            TRY_CATCH_CALL(async->stmt->handle(), cb, 2, argv);
        }
        uv_close(reinterpret_cast<uv_handle_t*>(handle), CloseCallback);
    }
}

void Statement::Work_AfterEach(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(EachBaton);

    if (stmt->status != SQLITE_DONE) {
        Error(baton);
    }

    STATEMENT_END();
}

NAPI_METHOD(Statement::Reset) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    GET_ARGUMENTS(1);
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(stmt, callback);
    stmt->Schedule(Work_BeginReset, baton);

    napi_set_return_value(env, info, _this);
}

void Statement::Work_BeginReset(Baton* baton) {
    STATEMENT_BEGIN(Reset);
}

void Statement::Work_Reset(uv_work_t* req) {
    STATEMENT_INIT(Baton);

    sqlite3_reset(stmt->_handle);
    stmt->status = SQLITE_OK;
}

void Statement::Work_AfterReset(uv_work_t* req) {
    Napi::HandleScope scope;

    STATEMENT_INIT(Baton);

    // Fire callbacks.
    napi_value cb = Napi::New(env, baton->callback);
    if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
        napi_value argv[] = { Napi::Null(env) };
        TRY_CATCH_CALL(stmt->handle(), cb, 1, argv);
    }

    STATEMENT_END();
}

napi_value Statement::RowToJS(Row* row) {
    Napi::EscapableHandleScope scope;
    napi_env env = napi_get_current_env();
    napi_value result = Napi::NewObject(env);

    Row::const_iterator it = row->begin();
    Row::const_iterator end = row->end();
    for (int i = 0; it < end; ++it, i++) {
        Values::Field* field = *it;

        napi_value value;

        switch (field->type) {
            case SQLITE_INTEGER: {
                value = Napi::New(env, (double)(((Values::Integer*)field)->value));
            } break;
            case SQLITE_FLOAT: {
                value = Napi::New(env, (double)((Values::Float*)field)->value);
            } break;
            case SQLITE_TEXT: {
                value = Napi::New(env, ((Values::Text*)field)->value.c_str(), ((Values::Text*)field)->value.size());
            } break;
            case SQLITE_BLOB: {
                value = Napi::CopyBuffer(env, ((Values::Blob*)field)->value, ((Values::Blob*)field)->length);
            } break;
            case SQLITE_NULL: {
                value = Napi::Null(env);
            } break;
        }
        Napi::Set(env, result, Napi::New(env, field->name.c_str()), value);

        DELETE_FIELD(field);
    }

    return scope.Escape(result);
}

void Statement::GetRow(Row* row, sqlite3_stmt* stmt) {
    int rows = sqlite3_column_count(stmt);

    for (int i = 0; i < rows; i++) {
        int type = sqlite3_column_type(stmt, i);
        const char* name = sqlite3_column_name(stmt, i);
        switch (type) {
            case SQLITE_INTEGER: {
                row->push_back(new Values::Integer(name, sqlite3_column_int64(stmt, i)));
            }   break;
            case SQLITE_FLOAT: {
                row->push_back(new Values::Float(name, sqlite3_column_double(stmt, i)));
            }   break;
            case SQLITE_TEXT: {
                const char* text = (const char*)sqlite3_column_text(stmt, i);
                int length = sqlite3_column_bytes(stmt, i);
                row->push_back(new Values::Text(name, length, text));
            } break;
            case SQLITE_BLOB: {
                const void* blob = sqlite3_column_blob(stmt, i);
                int length = sqlite3_column_bytes(stmt, i);
                row->push_back(new Values::Blob(name, length, blob));
            }   break;
            case SQLITE_NULL: {
                row->push_back(new Values::Null(name));
            }   break;
            default:
                assert(false);
        }
    }
}

NAPI_METHOD(Statement::Finalize) {
    napi_value _this = napi_get_cb_this(env, info);
    Statement* stmt = Napi::ObjectWrap::Unwrap<Statement>(_this);

    GET_ARGUMENTS(1);
    OPTIONAL_ARGUMENT_FUNCTION(0, callback);

    Baton* baton = new Baton(stmt, callback);
    stmt->Schedule(Finalize, baton);

    napi_set_return_value(env, info, stmt->db->handle());
}

void Statement::Finalize(Baton* baton) {
    Napi::HandleScope scope;
    napi_env env = napi_get_current_env();

    baton->stmt->Finalize();

    // Fire callback in case there was one.
    napi_value cb = Napi::New(env, baton->callback);
    if (!Napi::IsEmpty(env, cb) && Napi::IsFunction(env, cb)) {
        TRY_CATCH_CALL(baton->stmt->handle(), cb, 0, NULL);
    }

    delete baton;
}

void Statement::Finalize() {
    assert(!finalized);
    finalized = true;
    CleanQueue();
    // Finalize returns the status code of the last operation. We already fired
    // error events in case those failed.
    sqlite3_finalize(_handle);
    _handle = NULL;
    db->Unref();
}

void Statement::CleanQueue() {
    Napi::HandleScope scope;
    napi_env env = napi_get_current_env();

    if (prepared && !queue.empty()) {
        // This statement has already been prepared and is now finalized.
        // Fire error for all remaining items in the queue.
        EXCEPTION(Napi::New(env, "Statement is already finalized"), SQLITE_MISUSE, exception);
        napi_value argv[] = { exception };
        bool called = false;

        // Clear out the queue so that this object can get GC'ed.
        while (!queue.empty()) {
            Call* call = queue.front();
            queue.pop();

            napi_value cb = Napi::New(env, call->baton->callback);

            if (prepared && !Napi::IsEmpty(env, cb) &&
                Napi::IsFunction(env, cb)) {
                TRY_CATCH_CALL(handle(), cb, 1, argv);
                called = true;
            }

            // We don't call the actual callback, so we have to make sure that
            // the baton gets destroyed.
            delete call->baton;
            delete call;
        }

        // When we couldn't call a callback function, emit an error on the
        // Statement object.
        if (!called) {
            napi_value info[] = { Napi::New(env, "error"), exception };
            EMIT_EVENT(handle(), 2, info);
        }
    }
    else while (!queue.empty()) {
        // Just delete all items in the queue; we already fired an event when
        // preparing the statement failed.
        Call* call = queue.front();
        queue.pop();

        // We don't call the actual callback, so we have to make sure that
        // the baton gets destroyed.
        delete call->baton;
        delete call;
    }
}
