#ifndef NODE_SQLITE3_SRC_MACROS_H
#define NODE_SQLITE3_SRC_MACROS_H

const char* sqlite_code_string(int code);
const char* sqlite_authorizer_string(int type);

#define GET_ARGUMENTS(n)                                                       \
    napi_value args[n];                                                        \
    napi_get_cb_args(env, info, args, n);

#define REQUIRE_ARGUMENTS(n)                                                   \
    if (Napi::Length(info) < (n)) {                                            \
        return Napi::ThrowTypeError("Expected " #n "arguments");               \
    }


#define REQUIRE_ARGUMENT_EXTERNAL(i, var)                                      \
    if (Napi::Length(info) <= (i) || !Napi::IsExternal(args[i])) {             \
        return Napi::ThrowTypeError("Argument " #i " invalid");                \
    }                                                                          \
    napi_value var = args[i];


#define REQUIRE_ARGUMENT_FUNCTION(i, var)                                      \
    if (Napi::Length(info) <= (i) || !Napi::IsFunction(args[i])) {             \
        return Napi::ThrowTypeError("Argument " #i " must be a function");     \
    }                                                                          \
    napi_value var = args[i];


#define REQUIRE_ARGUMENT_STRING(i, var)                                        \
    if (Napi::Length(info) <= (i) || !Napi::IsString(args[i])) {               \
        return Napi::ThrowTypeError("Argument " #i " must be a string");       \
    }                                                                          \
    Napi::Utf8String var(args[i]);


#define OPTIONAL_ARGUMENT_FUNCTION(i, var)                                     \
    napi_value var = Napi::Value();                                            \
    if (Napi::Length(info) > i && !Napi::IsUndefined(args[i])) {               \
        if (!Napi::IsFunction(args[i])) {                                      \
            return Napi::ThrowTypeError("Argument " #i " must be a function"); \
        }                                                                      \
        var = args[i];                                                         \
    }


#define OPTIONAL_ARGUMENT_INTEGER(i, var, default)                             \
    int var;                                                                   \
    if (Napi::Length(info) <= (i)) {                                           \
        var = (default);                                                       \
    }                                                                          \
    else if (Napi::IsInt32(args[i])) {                                         \
        var = Napi::To<int32_t>(args[i]);                                      \
    }                                                                          \
    else {                                                                     \
        return Napi::ThrowTypeError("Argument " #i " must be an integer");     \
    }


#define DEFINE_CONSTANT_INTEGER(target, constant, name)                        \
    napi_set_property(env, target,                                             \
                      napi_property_name(env, #name),                          \
                      napi_create_number(env, constant)                        \
    );

#define DEFINE_CONSTANT_STRING(target, constant, name)                         \
    napi_set_property(env, target,                                             \
                      napi_property_name(env, #name),                          \
                      napi_create_string(env, constant)                        \
    );


#define NODE_SET_GETTER(target, name, function)                                \
    Napi::SetAccessor((target)->InstanceTemplate(),                            \
        Napi::New(name), (function));

#define GET_STRING(source, name, property)                                     \
    Napi::Utf8String name(Napi::Get(source,                                    \
        Napi::New(property)));

#define GET_INTEGER(source, name, prop)                                        \
    int name = Napi::To<int>(Napi::Get(source,                                 \
        Napi::New(property)));

#define EXCEPTION(msg, errno, name)                                            \
    napi_value name = Napi::Error(                                             \
        Napi::StringConcat(                                                    \
            Napi::StringConcat(                                                \
                Napi::New(sqlite_code_string(errno)),                          \
                Napi::New(": ")                                                \
            ),                                                                 \
            (msg)                                                              \
        )                                                                      \
    );                                                                         \
    napi_value name ##_obj = name;                                             \
    Napi::Set(name ##_obj, Napi::New("errno"), Napi::New(errno));              \
    Napi::Set(name ##_obj, Napi::New("code"),                                  \
        Napi::New(sqlite_code_string(errno)));


#define EMIT_EVENT(obj, argc, argv)                                            \
    TRY_CATCH_CALL((obj),                                                      \
        Napi::Get(obj,                                                         \
            Napi::New("emit")),                                                \
        argc, argv                                                             \
    );

#define TRY_CATCH_CALL(context, callback, argc, argv)                          \
    Napi::MakeCallback((context), (callback), (argc), (argv))

#define WORK_DEFINITION(name)                                                  \
    static NAPI_METHOD(name);                                                  \
    static void Work_Begin##name(Baton* baton);                                \
    static void Work_##name(uv_work_t* req);                                    \
    static void Work_After##name(uv_work_t* req);

#define STATEMENT_BEGIN(type)                                                  \
    assert(baton);                                                             \
    assert(baton->stmt);                                                       \
    assert(!baton->stmt->locked);                                              \
    assert(!baton->stmt->finalized);                                           \
    assert(baton->stmt->prepared);                                             \
    baton->stmt->locked = true;                                                \
    baton->stmt->db->pending++;                                                \
    int status = uv_queue_work(uv_default_loop(),                              \
        &baton->request,                                                       \
        Work_##type, reinterpret_cast<uv_after_work_cb>(Work_After##type));    \
    assert(status == 0);

#define STATEMENT_INIT(type)                                                   \
    type* baton = static_cast<type*>(req->data);                               \
    Statement* stmt = baton->stmt;

#define STATEMENT_END()                                                        \
    assert(stmt->locked);                                                      \
    assert(stmt->db->pending);                                                 \
    stmt->locked = false;                                                      \
    stmt->db->pending--;                                                       \
    stmt->Process();                                                           \
    stmt->db->Process();                                                       \
    delete baton;

#define DELETE_FIELD(field)                                                    \
    if (field != NULL) {                                                       \
        switch ((field)->type) {                                               \
            case SQLITE_INTEGER: delete (Values::Integer*)(field); break;      \
            case SQLITE_FLOAT:   delete (Values::Float*)(field); break;        \
            case SQLITE_TEXT:    delete (Values::Text*)(field); break;         \
            case SQLITE_BLOB:    delete (Values::Blob*)(field); break;         \
            case SQLITE_NULL:    delete (Values::Null*)(field); break;         \
        }                                                                      \
    }

#endif
