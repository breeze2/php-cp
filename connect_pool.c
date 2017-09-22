/*
  +----------------------------------------------------------------------+
  | common con pool                                                      |
  +----------------------------------------------------------------------+
  | This source file is subject to version 2.0 of the Apache license,    |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.apache.org/licenses/LICENSE-2.0.html                      |
  | If you did not receive a copy of the Apache2.0 license and are unable|
  | to obtain it through the world-wide-web, please send a note to       |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author: Xinhua Guo  <woshiguo35@sina.com>                            |
  +----------------------------------------------------------------------+
 */

#include "php_connect_pool.h"
#include <ext/standard/info.h>
#include "zend_interfaces.h"
#include "zend_exceptions.h"
#include <sys/time.h>
#include <Zend/zend_types.h>

#include <Zend/zend.h>

#include <ext/spl/spl_iterators.h>
static zval* pdo_stmt = NULL;
extern zval* pdo_object;
extern zval* redis_object;
extern zval* memcached_object;

cpServerG ConProxyG;
cpServerGS *ConProxyGS = NULL;
cpWorkerG ConProxyWG;

extern sapi_module_struct sapi_module;

static void cp_destory_client(zend_resource *rsrc TSRMLS_DC);

static void pdo_dispatch(zval *args);
static void pdo_proxy_pdo(zval *args);
static void pdo_proxy_stmt(zval *args);
static void cp_add_fail_into_mem(zval *conf, zval *data_source);

void cpLogVar(zval *var);

#define CP_VERSION "1.5.0"

#define CP_INTERNAL_ERROR_SEND(send_data)\
                                ({         \
                                CP_INTERNAL_SEND_RAW(send_data,CP_SIGEVENT_EXCEPTION)\
                                 })

#define CP_INTERNAL_NORMAL_SEND(send_data)\
                                ({         \
                                 CP_INTERNAL_SEND_RAW(send_data,CP_SIGEVENT_TURE)\
                                 })
#define CP_SEND_EXCEPTION do{zval *str;CP_SEND_EXCEPTION_ARGS(&str);cp_zval_ptr_dtor(&str);}while(0);
#define CP_INTERNAL_NORMAL_SEND_RETURN(send_data)({CP_INTERNAL_NORMAL_SEND(send_data);return CP_TRUE;})
#define CP_INTERNAL_ERROR_SEND_RETURN(send_data) ({ CP_INTERNAL_ERROR_SEND(send_data);return CP_FALSE;})
#define CP_SEND_EXCEPTION_RETURN do{CP_SEND_EXCEPTION;return CP_FALSE;}while(0);
#define CP_TEST_RETURN_TRUE(flag) ({if(flag==CP_CONNECT_PING)return CP_TRUE;})


const zend_function_entry cp_functions[] = {
    PHP_FE(pool_server_create, NULL)
    PHP_FE(pool_server_status, NULL)
    PHP_FE(pool_server_shutdown, NULL)
    PHP_FE(pool_server_reload, NULL)
    PHP_FE(pool_server_version, NULL)
    PHP_FE_END /* Must be the last line in cp_functions[] */
};

ZEND_BEGIN_ARG_INFO_EX(__call_args, 0, 0, 2)
ZEND_ARG_INFO(0, function_name)
ZEND_ARG_INFO(0, arguments)
ZEND_END_ARG_INFO()

const zend_function_entry pdo_connect_pool_methods[] = {
    PHP_ME(pdo_connect_pool, __construct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(pdo_connect_pool, __destruct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_DTOR)
    PHP_ME(pdo_connect_pool, __call, __call_args, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool, release, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool, close, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool, setAsync, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool, done, NULL, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

ZEND_BEGIN_ARG_INFO_EX(arginfo_statement_void, 0, 0, 0)
ZEND_END_ARG_INFO()

const zend_function_entry pdo_connect_pool_PDOStatement_methods[] = {
    PHP_ME(pdo_connect_pool_PDOStatement, __call, __call_args, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, setAsync, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, release, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, done, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, rewind, arginfo_statement_void, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, next, arginfo_statement_void, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, current, arginfo_statement_void, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, key, arginfo_statement_void, ZEND_ACC_PUBLIC)
    PHP_ME(pdo_connect_pool_PDOStatement, valid, arginfo_statement_void, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

PHP_METHOD(pdo_connect_pool_PDOStatement, rewind)
{
    zval *object = getThis();
    zval *method_ptr, method, *ret_value = NULL;
    method_ptr = &method;
    zend_class_entry *ce;
    ce = Z_OBJCE_P(object);
    CP_ZVAL_STRING(method_ptr, "fetchAll", 0);
    if (cp_call_user_function_ex(EG(function_table), &object, method_ptr, &ret_value, 0, NULL, 0, NULL TSRMLS_CC) == FAILURE)
    {
        return;
    }
    zend_update_property_long(ce, object, "pos", sizeof ("pos") - 1, 0 TSRMLS_CC);
    zend_update_property(ce, object, "rs", sizeof ("rs") - 1, ret_value TSRMLS_CC);
    cp_zval_ptr_dtor(&ret_value);

}

PHP_METHOD(pdo_connect_pool_PDOStatement, current)
{
    zval *pos, *rs, *row = NULL;
    zend_class_entry *ce;
    ce = Z_OBJCE_P(getThis());
    pos = cp_zend_read_property(ce, getThis(), "pos", sizeof ("pos") - 1, 0 TSRMLS_DC);
    rs = cp_zend_read_property(ce, getThis(), "rs", sizeof ("rs") - 1, 0 TSRMLS_DC);

    cp_zend_hash_index_find(Z_ARRVAL_P(rs), Z_LVAL_P(pos), (void**) &row);
    RETVAL_ZVAL(row, 1, 1);
}

PHP_METHOD(pdo_connect_pool_PDOStatement, key)
{
    zval *pos;
    zend_class_entry *ce;
    ce = Z_OBJCE_P(getThis());
    pos = cp_zend_read_property(ce, getThis(), "pos", sizeof ("pos") - 1, 0 TSRMLS_DC);
    ZVAL_LONG(return_value, Z_LVAL_P(pos));
}

PHP_METHOD(pdo_connect_pool_PDOStatement, next)
{
    zval *pos;
    zend_class_entry *ce;
    ce = Z_OBJCE_P(getThis());
    pos = cp_zend_read_property(ce, getThis(), "pos", sizeof ("pos") - 1, 0 TSRMLS_DC);

    zend_update_property_long(ce, getThis(), "pos", sizeof ("pos") - 1, ++Z_LVAL_P(pos) TSRMLS_CC);
}

PHP_METHOD(pdo_connect_pool_PDOStatement, valid)
{
    zval *pos, *rs, *row = NULL;
    zend_class_entry *ce;
    ce = Z_OBJCE_P(getThis());
    pos = cp_zend_read_property(ce, getThis(), "pos", sizeof ("pos") - 1, 0 TSRMLS_DC);
    rs = cp_zend_read_property(ce, getThis(), "rs", sizeof ("rs") - 1, 0 TSRMLS_DC);

    if (cp_zend_hash_index_find(Z_ARRVAL_P(rs), Z_LVAL_P(pos), (void**) &row) == SUCCESS)
    {
        RETURN_BOOL(1);
    }
    else
    {
        RETURN_BOOL(0);
    }

}
const zend_function_entry redis_connect_pool_methods[] = {
    PHP_ME(redis_connect_pool, __construct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(redis_connect_pool, __destruct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_DTOR)
    PHP_ME(redis_connect_pool, __call, __call_args, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, release, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, select, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, connect, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, auth, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, done, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, setAsync, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(redis_connect_pool, close, NULL, ZEND_ACC_PUBLIC)
    PHP_MALIAS(redis_connect_pool, pconnect, connect, NULL, ZEND_ACC_PUBLIC) /* pconnect 别名指向connect */
    PHP_FE_END
};

const zend_function_entry memcached_connect_pool_methods[] = {
    PHP_ME(memcached_connect_pool, __construct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_CTOR)
    PHP_ME(memcached_connect_pool, __destruct, NULL, ZEND_ACC_PUBLIC | ZEND_ACC_DTOR)
    PHP_ME(memcached_connect_pool, __call, __call_args, ZEND_ACC_PUBLIC)
    PHP_FE_END
};

int le_cp_server;
int le_cli_connect_pool;

zend_class_entry pdo_connect_pool_ce;
zend_class_entry *pdo_connect_pool_class_entry_ptr;

zend_class_entry redis_connect_pool_ce;
zend_class_entry *redis_connect_pool_class_entry_ptr;

zend_class_entry pdo_connect_pool_PDOStatement_ce;
zend_class_entry *pdo_connect_pool_PDOStatement_class_entry_ptr;

zend_class_entry memcached_connect_pool_ce;
zend_class_entry *memcached_connect_pool_class_entry_ptr;

zend_module_entry connect_pool_module_entry = {
#if ZEND_MODULE_API_NO >= 20050922
    STANDARD_MODULE_HEADER_EX,
    NULL,
    NULL,
#else
    STANDARD_MODULE_HEADER,
#endif
    "connect_pool",
    cp_functions,
    PHP_MINIT(connect_pool),
    PHP_MSHUTDOWN(connect_pool),
    PHP_RINIT(connect_pool), //RINIT
    PHP_RSHUTDOWN(connect_pool), //RSHUTDOWN
    PHP_MINFO(connect_pool),
    CP_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_CONNECT_POOL

ZEND_GET_MODULE(connect_pool)
#endif

PHP_MINIT_FUNCTION(connect_pool)
{
    le_cli_connect_pool = zend_register_list_destructors_ex(send_oob2proxy, cp_destory_client, CP_RES_CLIENT_NAME, module_number); //持久

    INIT_CLASS_ENTRY(pdo_connect_pool_ce, "pdoProxy", pdo_connect_pool_methods);
    pdo_connect_pool_class_entry_ptr = zend_register_internal_class(&pdo_connect_pool_ce TSRMLS_CC);
    zend_register_class_alias("pdo_connect_pool", pdo_connect_pool_class_entry_ptr);

    INIT_CLASS_ENTRY(redis_connect_pool_ce, "redisProxy", redis_connect_pool_methods);
    redis_connect_pool_class_entry_ptr = zend_register_internal_class(&redis_connect_pool_ce TSRMLS_CC);
    zend_register_class_alias("redis_connect_pool", redis_connect_pool_class_entry_ptr);

    INIT_CLASS_ENTRY(pdo_connect_pool_PDOStatement_ce, "pdo_connect_pool_PDOStatement", pdo_connect_pool_PDOStatement_methods);

    INIT_CLASS_ENTRY(memcached_connect_pool_ce, "memcachedProxy", memcached_connect_pool_methods);
    memcached_connect_pool_class_entry_ptr = zend_register_internal_class(&memcached_connect_pool_ce TSRMLS_CC);
    zend_register_class_alias("memcached_connect_pool", memcached_connect_pool_class_entry_ptr);

    //zend_class_entry *pdo_dbstmt_ce = cp_zend_fetch_class("PDOStatement", ZEND_FETCH_CLASS_AUTO);

    pdo_connect_pool_PDOStatement_class_entry_ptr = zend_register_internal_class(&pdo_connect_pool_PDOStatement_ce TSRMLS_CC);
    zend_class_implements(pdo_connect_pool_PDOStatement_class_entry_ptr TSRMLS_CC, 1, spl_ce_Iterator, spl_ce_Countable);


    REGISTER_LONG_CONSTANT("CP_DEFAULT_PDO_PORT", CP_PORT_PDO, CONST_CS | CONST_PERSISTENT);
    REGISTER_LONG_CONSTANT("CP_DEFAULT_REDIS_PORT", CP_PORT_REDIS, CONST_CS | CONST_PERSISTENT);

    bzero(&ConProxyG, sizeof (ConProxyG));
    bzero(&ConProxyWG, sizeof (ConProxyWG));

    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(connect_pool)
{
    if (pdo_stmt)
    {
        cp_zval_ptr_dtor(&pdo_stmt);
    }
    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(connect_pool)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "connect_pool support", "enabled");
    php_info_print_table_row(2, "Version", CP_VERSION);
    php_info_print_table_row(2, "Author", "郭新华，张磊");
    php_info_print_table_row(2, "email", "woshiguo35@sina.com");
    php_info_print_table_end();
}

/* }}} */

PHP_RINIT_FUNCTION(connect_pool)
{
    return SUCCESS;
}

PHP_RSHUTDOWN_FUNCTION(connect_pool)
{
    return SUCCESS;
}

static void cp_destory_client(zend_resource *rsrc TSRMLS_DC)
{
    cpClient *cli = (cpClient *) rsrc->ptr;
    if (cli->sock > 0)
    {
        cpClient_close(cli);
    }
}

void send_oob2proxy(zend_resource *rsrc TSRMLS_DC)
{
    cpClient *cli = (cpClient *) rsrc->ptr;
    if (cli->sock == 0)
    {
        pefree(cli, 1);
    }
    else if (cli->server_fd != 0)
    {//防止release后rshutdown的重复释放
        cpConnection *conn = CONN(cli);
        if (conn->release == CP_FD_NRELEASED)
        {
            cpGroup* G = &CPGS->G[conn->group_id];
            if (cli->lock(G) == 0)
            {
                conn->release = CP_FD_RELEASED;
                if (G->first_wait_id && conn->worker_index <= G->worker_max)
                {//wait is not null&&use queue&&use reload to reduce max maybe trigger this
                    int wait_pid = cpPopWaitQueue(G, conn);
                    cli->unLock(G);
                    if (kill(wait_pid, SIGRTMIN) < 0)
                    {
                        php_printf("send sig 2 %d error. Error: %s [%d]", wait_pid, strerror(errno), errno);
                        //                        return send_oob2proxy(rsrc);

                    }
                }
                else
                {
                    CPGS->G[conn->group_id].workers_status[conn->worker_index] = CP_WORKER_IDLE;
                    cli->unLock(G);
                }

            }
            log_end(cli);
        }
    }
}

PHP_FUNCTION(pool_server_create)
{
    zval *conf = NULL;
    char *config_file = NULL;
    //zend_string *config_file = NULL;
    zend_size_t file_len = 0;
    if (strcasecmp("cli", sapi_module.name) != 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "pool_server must run at php_cli environment.");
        RETURN_FALSE;
    }
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &config_file, &file_len) == FAILURE)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "config file error");
        RETURN_FALSE;
    }
    conf = cpGetConfig(config_file);
    int sock = cpServer_init(conf, config_file);
    if (sock <= 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "pool_server: start server fail. Error: %s [%d]", strerror(errno), errno);
    }

    int ret = cpServer_create();
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "pool_server: create server fail. Error: %s [%d]", strerror(errno), errno);
    }

    ret = cpServer_start(sock);
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "pool_server: start server fail. Error: %s [%d]", strerror(errno), errno);
    }
    cp_zval_ptr_dtor(&conf);
}

PHP_FUNCTION(pool_server_reload)
{
    long pid;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l", &pid) == FAILURE)
    {
        return;
    }
    if (kill(pid, SIGUSR1) < 0)
    {
        php_printf("reload fail. kill -SIGUSR1 master_pid[%d] fail. Error: %s[%d]\n", (int) pid, strerror(errno), errno);
        RETURN_FALSE;
    }
    else
    {
        RETURN_TRUE;
    }
}

PHP_FUNCTION(pool_server_version)
{
    CP_RETURN_STRING(CP_VERSION, 1);
}

PHP_FUNCTION(pool_server_shutdown)
{
    long pid;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l", &pid) == FAILURE)
    {
        return;
    }
    if (kill(pid, SIGTERM) < 0)
    {
        php_printf("shutdown fail. kill -SIGTERM master_pid[%d] fail. Error: %s[%d]\n", (int) pid, strerror(errno), errno);
        RETURN_FALSE;
    }
    else
    {
        RETURN_TRUE;
    }
}

int CP_INTERNAL_SERIALIZE_SEND_MEM(zval *send_data, uint8_t __type)
{
    cpShareMemory *sm_obj = &(CPGS->G[CPWG.gid].workers[CPWG.id].sm_obj);
    int real_len = 0;

    instead_smart dest;
    dest.len = 0;
    dest.addr = sm_obj->mem;
    dest.max = CPGC.max_read_len;
    dest.exceed = 0;
    php_msgpack_serialize(&dest, send_data);
    real_len = dest.len;
    if (dest.exceed == 1)
    {
        CP_INTERNAL_ERROR_SEND("data is exceed,increase max_read_len");
        return SUCCESS;
    }

    cpWorkerInfo worker_event;
    worker_event.len = real_len;
    worker_event.type = __type;
    worker_event.pid = CPWG.event.pid;
    int ret = write(CPWG.pipe_fd_write, &worker_event, sizeof (worker_event));
    if (ret == -1)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "write error Error: %s [%d]", strerror(errno), errno);
    }

    return SUCCESS;
}

int pdo_proxy_connect(zval *args, int flag)
{
    zval *data_source, **tmp_pass[4], *new_obj, *username, *password, *options, * ret_pdo_obj, *null_arr = NULL;
    zval pdo_name, con_fun_name;
    zend_class_entry *pdo_ce;
    if (pdo_object)
    {
        pdo_proxy_pdo(args);
        return 1;
    }
#if PHP_MAJOR_VERSION < 7
    CP_MAKE_STD_ZVAL(new_obj);
#else
    new_obj = ecalloc(sizeof (zval), 1);
#endif
    CP_ZVAL_STRING(&pdo_name, "pdo", 0);
    if (cp_zend_hash_find_ptr(EG(class_table), &pdo_name, (void **) &pdo_ce) == FAILURE)
    {
        CP_INTERNAL_ERROR_SEND_RETURN("pdo extension is not install");
    }
    object_init_ex(new_obj, pdo_ce);
    cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("data_source"), (void **) &data_source);
    tmp_pass[0] = &data_source;
    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("username"), (void **) &username) == SUCCESS)
    {
        tmp_pass[1] = &username;
    }

    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("password"), (void **) &password) == SUCCESS)
    {
        tmp_pass[2] = &password;
    }

    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("options"), (void **) &options) == SUCCESS)
    {
        tmp_pass[3] = &options;
    }
    else
    {
        CP_MAKE_STD_ZVAL(null_arr);
        array_init(null_arr);
        tmp_pass[3] = &null_arr;
    }

    CP_ZVAL_STRING(&con_fun_name, "__construct", 0);
    cp_call_user_function_ex(NULL, &new_obj, &con_fun_name, &ret_pdo_obj, 4, tmp_pass, 0, NULL TSRMLS_CC);
#if PHP_MAJOR_VERSION ==7
    zval_ptr_dtor(&con_fun_name);
#endif
    if (null_arr)
        cp_zval_ptr_dtor(&null_arr);
    if (ret_pdo_obj)
        cp_zval_ptr_dtor(&ret_pdo_obj);
    if (EG(exception))
    {
        CP_DEL_OBJ(new_obj);
        CP_SEND_EXCEPTION_RETURN;
    }
    else
    {
        pdo_object = new_obj;
        if (flag == CP_CONNECT_NORMAL)
        {
            pdo_proxy_pdo(args);
            return 1;
        }
    }
    return CP_FALSE;
}

static void pdo_proxy_pdo(zval * args)
{
    zval *str = NULL, *method = NULL, * ret_value = NULL;
    if (pdo_object)
    {
        if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("method"), (void **) &method) == FAILURE)
        {
            CP_INTERNAL_ERROR_SEND("PDO no method!");
        }
#if PHP_MAJOR_VERSION ==7
        ret_value = (zval *) emalloc(sizeof (zval));
#endif

        if (cp_internal_call_user_function(pdo_object, method, &ret_value, args) == FAILURE)
        {
            CP_DEL_OBJ(pdo_object);
            char cp_error_str[FAILUREOR_MSG_SIZE] = {0};
            snprintf(cp_error_str, FAILUREOR_MSG_SIZE, "call pdo method( %s ) error!", Z_STRVAL_P(method));
            CP_INTERNAL_ERROR_SEND(cp_error_str);
        }
        else
        {
            if (EG(exception))
            {
                CP_EXCEPTION_ARGS(&str);
                char *p = strcasestr(Z_STRVAL_P(str), "server has gone away");
                char *p2 = strcasestr(Z_STRVAL_P(str), "There is already an active transaction");
                if (p || p2)
                {//del reconnect and retry
                    cpLog("del and retry %s,%s", p, p2);
                    CP_DEL_OBJ(pdo_object);
                    pdo_proxy_connect(args, CP_CONNECT_NORMAL);
                }
                else
                {
                    CP_INTERNAL_SERIALIZE_SEND_MEM(str, CP_SIGEVENT_EXCEPTION);
                }
                cp_zval_ptr_dtor(&str);
                if (ret_value)
                {
                    cp_zval_ptr_dtor(&ret_value);
#if PHP_MAJOR_VERSION == 7
                    efree(ret_value);
#endif
                }
            }
            else
            {
                if (Z_TYPE_P(ret_value) == IS_OBJECT)
                {
#if PHP_MAJOR_VERSION < 7
                    char *name;
                    zend_uint name_len;
                    zend_get_object_classname(ret_value, (const char **) &name, &name_len TSRMLS_CC);
                    if (strcmp(name, "PDOStatement") == 0)
                    {
                        if (pdo_stmt)
                        {
                            zval_ptr_dtor(&pdo_stmt);
                            pdo_stmt = NULL;
                        }
                        pdo_stmt = ret_value;
                        zval send_zval = {0};
                        ZVAL_STRING(&send_zval, "PDOStatement!", 0);
                        CP_INTERNAL_SERIALIZE_SEND_MEM(&send_zval, CP_SIGEVENT_PDO);
                    }
                    efree(name);
#else
                    zend_string *name = Z_OBJ_HANDLER_P(ret_value, get_class_name)(Z_OBJ_P(ret_value));
                    if (strcmp(name->val, "PDOStatement") == 0)
                    {
                        if (pdo_stmt)
                        {
                            zval_dtor(pdo_stmt);
                            efree(pdo_stmt);
                            pdo_stmt = NULL;
                        }
                        pdo_stmt = ret_value;
                        zval send_zval = {0};
                        CP_ZVAL_STRING(&send_zval, "PDOStatement!", 0);
                        CP_INTERNAL_SERIALIZE_SEND_MEM(&send_zval, CP_SIGEVENT_PDO);
                        zval_ptr_dtor(&send_zval);
                    }
                    zend_string_release(name);
#endif
                }
                else
                {//pdo
                    CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_TURE);
                    if (ret_value)
                    {
                        cp_zval_ptr_dtor(&ret_value);
#if PHP_MAJOR_VERSION == 7
                        efree(ret_value);
#endif
                    }
                }
            }
        }
    }
    else
    {
        CP_INTERNAL_ERROR_SEND("no connect to mysql");
    }
}

static void pdo_proxy_stmt(zval * args)
{
    zval *method = NULL, * ret_value = NULL;
    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("method"), (void **) &method) == FAILURE)
    {
        CP_INTERNAL_ERROR_SEND("PDO no method!");
    }
    if (cp_internal_call_user_function(pdo_stmt, method, &ret_value, args) == FAILURE)
    {
        char cp_error_str[FAILUREOR_MSG_SIZE] = {0};
        snprintf(cp_error_str, FAILUREOR_MSG_SIZE, "call pdo stmt method (%s) error!", Z_STRVAL_P(method));
        CP_INTERNAL_ERROR_SEND(cp_error_str);
    }
    else
    {
        if (EG(exception))
        {
            zval *str;
            CP_SEND_EXCEPTION_ARGS(&str);
            char *p = strcasestr(Z_STRVAL_P(str), "server has gone away");
            char *p2 = strcasestr(Z_STRVAL_P(str), "There is already an active transaction");
            if (p || p2)
            {
                CP_DEL_OBJ(pdo_object);
            }
            cp_zval_ptr_dtor(&str);
            cp_zval_ptr_dtor(&pdo_stmt);
            pdo_stmt = NULL;
            return; //when the exception,the ret_value dont need dtor
        }
        if (!ret_value)
        {
            CP_INTERNAL_ERROR_SEND("call pdo stmt method error ret_value is null!");
            return;
        }
        if (Z_TYPE_P(ret_value) == IS_OBJECT)
        {
            CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_STMT_OBJ);
        }
        else
        {
            CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_TURE);
        }
    }
    if (ret_value)
    {

        cp_zval_ptr_dtor(&ret_value);
    }
}

static void pdo_dispatch(zval * args)
{
    zval *m_type;
    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("method_type"), (void **) &m_type) == SUCCESS)
    {
        if (strcmp(Z_STRVAL_P(m_type), "connect") == 0)
        {
            pdo_proxy_connect(args, CP_CONNECT_NORMAL);
        }
        else if (strcmp(Z_STRVAL_P(m_type), "PDOStatement") == 0)
        {
            pdo_proxy_stmt(args);
        }
        //        else
        //        {//not use now
        //            pdo_proxy_pdo(args);
        //        }
    }
    else
    {//操作pdo

        CP_INTERNAL_ERROR_SEND("PDO  method_type is none!");
    }
}

static int cp_redis_select(zval *new_obj, zval **db)
{
    //有db并且不0那么就select
    if (strcmp("0", Z_STRVAL_PP(db)) != 0)
    {
        zval **tmp_pass[1];
        tmp_pass[0] = db;
        zval * ret_redis_select = NULL;
        zval select_fun_name;
        CP_ZVAL_STRING(&select_fun_name, "select", 0);
        cp_call_user_function_ex(NULL, &new_obj, &select_fun_name, &ret_redis_select, 1, tmp_pass, 0, NULL TSRMLS_CC);
        if (ret_redis_select)
            cp_zval_ptr_dtor(&ret_redis_select);

        if (EG(exception))
        {
            cp_zval_ptr_dtor(&new_obj);
            CP_SEND_EXCEPTION_RETURN;
        }
    }
    return CP_TRUE;
}

static int cp_redis_auth(zval *new_obj, zval *args)
{
    zval *auth, *ret_redis_auth = NULL;
    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("auth"), (void **) &auth) == SUCCESS)
    {
        zval auth_fun_name;
        CP_ZVAL_STRING(&auth_fun_name, "auth", 0);
        zval **tmp_pass[1];
        tmp_pass[0] = &auth;
        cp_call_user_function_ex(NULL, &new_obj, &auth_fun_name, &ret_redis_auth, 1, tmp_pass, 0, NULL TSRMLS_CC);
        if (ret_redis_auth)
        {
            cp_zval_ptr_dtor(&ret_redis_auth);
        }

        if (EG(exception))
        {
            cp_zval_ptr_dtor(&new_obj);
            CP_SEND_EXCEPTION_RETURN;
        }
    }
    return CP_TRUE;
}

int redis_proxy_connect(zval *args, int flag)
{
    zval *ex_arr, *data_source, zdelim, *ip, *port, *db, *timeout, * ret_redis_obj = NULL, *new_obj, **tmp_pass[3];
    cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("data_source"), (void **) &data_source);
    CP_MAKE_STD_ZVAL(ex_arr);
    array_init(ex_arr);
    CP_ZVAL_STRINGL(&zdelim, ":", 1, 0);
    cp_explode(&zdelim, data_source, ex_arr, LONG_MAX);

#if PHP_MAJOR_VERSION < 7
    CP_MAKE_STD_ZVAL(new_obj);
#else
    new_obj = ecalloc(sizeof (zval), 1);
#endif
    zend_class_entry *redis_ce = NULL;

    zval redis_name;
    CP_ZVAL_STRING(&redis_name, "redis", 0);
    if (cp_zend_hash_find_ptr(EG(class_table), &redis_name, (void **) &redis_ce) == FAILURE)
    {
        CP_INTERNAL_ERROR_SEND_RETURN("redis extension is not install");
    }
    object_init_ex(new_obj, redis_ce);
    if (cp_zend_hash_index_find(Z_ARRVAL_P(ex_arr), 0, (void**) &ip) == SUCCESS)
    {
        tmp_pass[0] = &ip;
    }
    if (cp_zend_hash_index_find(Z_ARRVAL_P(ex_arr), 1, (void**) &port) == SUCCESS)
    {
        tmp_pass[1] = &port;
    }

    CP_MAKE_STD_ZVAL(timeout);
    CP_ZVAL_STRING(timeout, "10", 0);
    tmp_pass[2] = &timeout;

    zval pcon_fun_name;
    CP_ZVAL_STRING(&pcon_fun_name, "connect", 0);

    cp_call_user_function_ex(NULL, &new_obj, &pcon_fun_name, &ret_redis_obj, 3, tmp_pass, 0, NULL TSRMLS_CC);
    if (ret_redis_obj)
    {
        if (Z_BVAL_P(ret_redis_obj) == FALSE)
        {
            cp_zval_ptr_dtor(&ex_arr);
            cp_zval_ptr_dtor(&ret_redis_obj);
            CP_INTERNAL_ERROR_SEND_RETURN("connect redis error!");
        }
        else
        {
            cp_zval_ptr_dtor(&ret_redis_obj);
        }
    }
    if (EG(exception))
    {
        CP_DEL_OBJ(new_obj);
        cp_zval_ptr_dtor(&ex_arr);
        CP_SEND_EXCEPTION_RETURN;
    }

    if (!cp_redis_auth(new_obj, args))
    {
        return CP_FALSE;
    }

    if (cp_zend_hash_index_find(Z_ARRVAL_P(ex_arr), 2, (void**) &db) == SUCCESS)
    {
        if (!cp_redis_select(new_obj, &db))
        {
            cp_zval_ptr_dtor(&ex_arr);
            return CP_FALSE;
        }
    }
    cp_zval_ptr_dtor(&ex_arr);
    //存起來
    redis_object = new_obj;
    zval *method;
    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("method"), (void **) &method) == FAILURE)
    {
        CP_INTERNAL_ERROR_SEND_RETURN("redis no method!");
    }
    if (strcmp(Z_STRVAL_P(method), "select") == 0)
    {
        CP_INTERNAL_NORMAL_SEND_RETURN("CON_SUCCESS!");
    }
    else
    {
        zval * ret_value = NULL;
        if (cp_internal_call_user_function(new_obj, method, &ret_value, args) == SUCCESS)
        {
            if (!EG(exception))
            {
                CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_TURE);

            }
            else
            {
                CP_DEL_OBJ(redis_object);
                CP_SEND_EXCEPTION;
            }
        }
        else
        {
            CP_INTERNAL_ERROR_SEND("call redis method error!");
        }
        if (ret_value)
            cp_zval_ptr_dtor(&ret_value);

        return CP_TRUE; //no use
    }
}

static void redis_dispatch(zval * args)
{
    zval *data_source;
    if (redis_object)
    {
        zval *method;
        if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("method"), (void **) &method) == FAILURE)
        {
            CP_INTERNAL_ERROR_SEND("redis no method error!");
            return;
        }
        else if (strcmp(Z_STRVAL_P(method), "select") == 0)
        {
            zval select_return = {0};
            ZVAL_BOOL(&select_return, 1);
            CP_INTERNAL_SERIALIZE_SEND_MEM(&select_return, CP_SIGEVENT_TURE);
            return;
        }
        zval * ret_value = NULL;
        if (cp_internal_call_user_function(redis_object, method, &ret_value, args) == FAILURE)
        {
            CP_INTERNAL_ERROR_SEND("call redis method error!");
        }
        else
        {
            if (EG(exception))
            {
                zval *str;
                CP_SEND_EXCEPTION_ARGS(&str);
                //                    char *p = strstr(Z_STRVAL_P(str), "server went away");
                //                    char *p2 = strstr(Z_STRVAL_P(str), "Connection lost");
                //                    char *p3 = strstr(Z_STRVAL_P(str), "read error on connection");
                //                    char *p4 = strstr(Z_STRVAL_P(str), "Connection closed");
                //                    if (p || p2 || p3 || p4)
                //                    {
                CP_DEL_OBJ(redis_object);
                // }
                cp_zval_ptr_dtor(&str);
            }
            else
            {
                CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_TURE);
            }
        }
        if (ret_value)
            cp_zval_ptr_dtor(&ret_value);
    }
    else
    {
        redis_proxy_connect(args, CP_CONNECT_NORMAL);
    }
}

static void memcached_dispatch(zval *args)
{
    if (!memcached_object)
    {
        zval *new_obj;
#if PHP_MAJOR_VERSION < 7
        CP_MAKE_STD_ZVAL(new_obj);
#else
        new_obj = ecalloc(sizeof (zval), 1);
#endif
        zend_class_entry *memcached_ce = NULL;

        zval memcached_name, fun_name, *ret_mem_obj;
        CP_ZVAL_STRING(&memcached_name, "memcached", 0);
        if (cp_zend_hash_find_ptr(EG(class_table), &memcached_name, (void **) &memcached_ce) == FAILURE)
        {
            CP_INTERNAL_ERROR_SEND_RETURN("memcached extension is not install");
        }
        object_init_ex(new_obj, memcached_ce);
        CP_ZVAL_STRING(&fun_name, "__construct", 0);
        cp_call_user_function_ex(NULL, &new_obj, &fun_name, &ret_mem_obj, 0, NULL, 0, NULL TSRMLS_CC);

        if (EG(exception))
        {
            CP_DEL_OBJ(new_obj);
            CP_SEND_EXCEPTION_RETURN;
        }

        //maybe need auth
        memcached_object = new_obj;
    }

    zval *method;
    if (cp_zend_hash_find(Z_ARRVAL_P(args), ZEND_STRS("method"), (void **) &method) == FAILURE)
    {
        CP_INTERNAL_ERROR_SEND("memcached no method error!");
        return;
    }

    zval * ret_value = NULL;
    if (cp_internal_call_user_function(memcached_object, method, &ret_value, args) == FAILURE)
    {
        CP_INTERNAL_ERROR_SEND("call memcached method error!");
    }
    else
    {
        if (EG(exception))
        {
            zval *str;
            CP_SEND_EXCEPTION_ARGS(&str);
            CP_DEL_OBJ(memcached_object);
            cp_zval_ptr_dtor(&str);
        }
        else
        {
            if (Z_TYPE_P(ret_value) == IS_OBJECT)
            {
                CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_STMT_OBJ);
            }
            else
            {
                CP_INTERNAL_SERIALIZE_SEND_MEM(ret_value, CP_SIGEVENT_TURE);
            }
        }
    }

    if (ret_value)
        cp_zval_ptr_dtor(&ret_value);
}

int worker_onReceive(zval * user_value)
{
    zval *type;
    if (cp_zend_hash_find(Z_ARRVAL_P(user_value), ZEND_STRS("type"), (void **) &type) == SUCCESS)
    {
        if (strcmp(Z_STRVAL_P(type), "pdo") == 0)
        {
            pdo_dispatch(user_value);
        }
        else if (strcmp(Z_STRVAL_P(type), "redis") == 0)
        {
            redis_dispatch(user_value);
        }
        else if (strcmp(Z_STRVAL_P(type), "memcached") == 0)
        {
            memcached_dispatch(user_value);
        }
        else
        {
            cpLog("wrong type");
        }
    }
    else
    {
        cpLog("args error no type!");
    }
    cp_zval_ptr_dtor(&user_value);

    return CP_TRUE;
}

static void cp_add_fail_into_mem(zval *o_arg, zval * data_source)
{
    zval *args;
    CP_MAKE_STD_ZVAL(args);
    *args = *o_arg;
    zval_copy_ctor(args);
    if (!CPGL.ping_mem_addr)
    {
        CPGL.ping_mem_addr = CPGS->ping_workers->sm_obj.mem;
    }
    zval *arr = CP_PING_GET_PRO(CPGL.ping_mem_addr);
    if (Z_TYPE_P(arr) == IS_NULL)
    {
        zval first_arr;
        array_init(&first_arr);
        add_assoc_long(args, "count", 1);
        add_assoc_zval(&first_arr, Z_STRVAL_P(data_source), args);
        cp_ser_and_setpro(&first_arr);
        zval_dtor(&first_arr);
    }
    else if (Z_TYPE_P(arr) != IS_TRUE)
    {
        zval **zval_source;
        if (cp_zend_hash_find(Z_ARRVAL_P(arr), Z_STRVAL_P(data_source), Z_STRLEN_P(data_source) + 1, (void **) &zval_source) == SUCCESS)
        {//++
            zval **zval_probably_count;
            if (cp_zend_hash_find(Z_ARRVAL_PP(zval_source), ZEND_STRS("count"), (void **) &zval_probably_count) == SUCCESS)
            {
                int num = (int) Z_LVAL_PP(zval_probably_count);
                add_assoc_long(args, "count", ++num);
                cp_zend_hash_del(Z_ARRVAL_P(arr), Z_STRVAL_P(data_source), Z_STRLEN_P(data_source));
                add_assoc_zval(arr, Z_STRVAL_P(data_source), args);
                cp_ser_and_setpro(arr);
            }
        }
        else
        {//add
            add_assoc_long(args, "count", 1);
            add_assoc_zval(arr, Z_STRVAL_P(data_source), args);
            cp_ser_and_setpro(arr);
        }

    }
    cp_zval_ptr_dtor(&arr);
}
