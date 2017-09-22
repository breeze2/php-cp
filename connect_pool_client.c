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
#include "php_streams.h"
#include "php_network.h"
#include "ext/standard/basic_functions.h"


extern zend_class_entry *pdo_connect_pool_class_entry_ptr;
extern zend_class_entry *redis_connect_pool_class_entry_ptr;
extern zend_class_entry *memcached_connect_pool_class_entry_ptr;
extern zend_class_entry *pdo_connect_pool_PDOStatement_class_entry_ptr;

cpRecvEvent RecvData;
static int *workerid2writefd = NULL;
static int *workerid2readfd = NULL;
static void **semid2attbuf = NULL;
static int cpPid = 0;
static int manager_pid = 0;
static int dev_random_fd = -1;
static uint16_t dummy_source_index = 0; //for multi async fun and the same source

static void cpClient_weekup(int sig)
{// do nothing now
}

static void cpClient_attach_mem()
{
    if (!CPGS)
    {
        int fd = open(CP_SERVER_MMAP_FILE, O_RDWR);
        if (fd == -1)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "open error Error: %s [%d],%s", strerror(errno), errno, CP_SERVER_MMAP_FILE);
        }
        if ((CPGS = mmap(NULL, sizeof (cpServerGS), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) < 0)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "attach sys mem error Error: %s [%d]", strerror(errno), errno);
        }
        manager_pid = CPGS->manager_pid;
        cpSignalSet(SIGRTMIN, cpClient_weekup, 1, 0);
    }
}

static void* connect_pool_perisent(zval* zres, zval* data_source)
{
    //    cpLog_init("/tmp/fpmlog");
    zend_resource sock_le;
    int ret;
    char *pool_server;
    cpClient* cli = (cpClient*) pecalloc(sizeof (cpClient), 1, 1);
    if (cpClient_create(cli) < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "pdo_connect_pool: create sock fail. Error: %s [%d]", strerror(errno), errno);
    }
    if (!(pool_server = getenv("POOL_SERVER")))
    {
        pool_server = "127.0.0.1";
    }
    ret = cpClient_connect(cli, pool_server, CP_PORT, (float) 100); //所有的操作100s超时
    if (ret < 0)
    {
        pefree(cli, 1);
        return NULL;
    }
    sock_le.type = le_cli_connect_pool;
    sock_le.ptr = cli;
    CP_ZEND_REGISTER_RESOURCE(zres, cli, le_cli_connect_pool);
#if PHP_MAJOR_VERSION < 7
    cp_zend_hash_update(&EG(persistent_list), Z_STRVAL_P(data_source), Z_STRLEN_P(data_source), (void*) &sock_le, sizeof (zend_resource), NULL);
#else 
    zend_hash_update_mem(&EG(persistent_list), Z_STR_P(data_source), (void *) &sock_le, sizeof (zend_resource));
#endif
    cli->lock = cpMutexLock;
    cli->unLock = cpMutexUnLock;

    cpTcpEvent event = {0};
    event.type = CP_TCPEVENT_GETFD;
    event.data = cpPid;
    cpClient_send(cli->sock, (char *) &event, sizeof (event), 0);
    cpMasterInfo info;
    ret = cpClient_recv(cli->sock, &info, sizeof (cpMasterInfo), 1);
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "recv from pool server error  [%d],%s", errno, strerror(errno));
    }
    cli->server_fd = info.server_fd;
    cpClient_attach_mem();
    CONN(cli)->release = CP_FD_RELEASED;
    bzero(&cli->big_data_tmp, sizeof (cli->big_data_tmp));
    bzero(&cli->slow_log_tmp, sizeof (cli->slow_log_tmp));
    return cli;
}

static CPINLINE zval * cpConnect_pool_server(zval *data_source, int async)
{
    cpClient *cli = NULL;
    zend_resource *p_sock_le;
    zval *zres = NULL;
    if (async)
    {//make dummy source
        char real_source[CP_SOURCE_MAX] = {0};
        zval dummy_source_zval;
        memcpy(real_source, Z_STRVAL_P(data_source), Z_STRLEN_P(data_source));
        sprintf(real_source + Z_STRLEN_P(data_source), "_%d", dummy_source_index);
        CP_ZVAL_STRING(&dummy_source_zval, real_source, 0);
        data_source = &dummy_source_zval;
        dummy_source_index++;
    }
#if PHP_MAJOR_VERSION < 7
    CP_ALLOC_INIT_ZVAL(zres);
    if (zend_hash_find(&EG(persistent_list), Z_STRVAL_P(data_source), Z_STRLEN_P(data_source), (void **) &p_sock_le) == SUCCESS)
#else 
    zres = (zval *) emalloc(sizeof (zval));
    if (cp_zend_hash_find_ptr(&EG(persistent_list), data_source, (void **) &p_sock_le) == SUCCESS)
#endif
    {
        cli = (cpClient*) p_sock_le->ptr;
        CP_ZEND_REGISTER_RESOURCE(zres, cli, le_cli_connect_pool);
    }
    else
    {//create long connect to pool_server
        if (connect_pool_perisent(zres, data_source) == NULL)
        {// error
#if PHP_MAJOR_VERSION < 7
            efree(zres);
#endif
            php_error_docref(NULL TSRMLS_CC, E_ERROR, CON_FAIL_MESSAGE);
            return NULL;
        }
    }
    return zres;
}

static void release_worker(zval *object)
{
    zend_resource *p_sock_le;
    zval *data_source;
    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("data_source"), (void **) &data_source) == SUCCESS)
    {
#if PHP_MAJOR_VERSION < 7
        if (zend_hash_find(&EG(persistent_list), Z_STRVAL_P(data_source), Z_STRLEN_P(data_source), (void **) &p_sock_le) == SUCCESS)
#else
        if (cp_zend_hash_find_ptr(&EG(persistent_list), data_source, (void **) &p_sock_le) == SUCCESS)
#endif
        {
            send_oob2proxy(p_sock_le);
        }
    }
    else
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "data_source can not find,we should do something like query before we release");
    }
}

static void close_conn(zval *object)
{//close the tcp with pool server
    zval *zres, *data_source;
    cpClient *cli;
    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("data_source"), (void **) &data_source) == SUCCESS)
    {
        if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("cli"), (void **) &zres) == SUCCESS)
        {
            char source_char[100] = {0};
            CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
            strcat(source_char, Z_STRVAL_P(data_source));
            if (cli->async)
            {
                sprintf(source_char + strlen(source_char), "_%d", cli->dummy_source_index);
            }
            cp_zend_hash_del(&EG(persistent_list), source_char, strlen(source_char));
        }
    }
}

static int get_writefd(int worker_id)
{
    if (workerid2writefd == NULL)
    {
        workerid2writefd = (int *) calloc(CP_GROUP_NUM*CP_GROUP_LEN, sizeof (int));
        if (workerid2writefd == NULL)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "calloc Error: %s [%d]", strerror(errno), errno);
        }
    }
    int pipe_fd_write;
    if (workerid2writefd[worker_id] == 0)
    {
        char file_c2w[CP_FIFO_NAME_LEN] = {0};
        sprintf(file_c2w, "%s_%d", CP_FIFO_NAME_PRE, worker_id);
        pipe_fd_write = cpCreateFifo(file_c2w);
        if (pipe_fd_write < 0)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "pipe open Error: %s [%d]", strerror(errno), errno);
        }
        workerid2writefd[worker_id] = pipe_fd_write;
    }
    else
    {
        pipe_fd_write = workerid2writefd[worker_id];
    }
    return pipe_fd_write;
}

static int get_readfd(int worker_id)
{
    if (workerid2readfd == NULL)
    {
        workerid2readfd = (int *) calloc(CP_GROUP_NUM*CP_GROUP_LEN, sizeof (int));
        if (workerid2readfd == NULL)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "calloc Error: %s [%d]", strerror(errno), errno);
        }
    }
    int pipe_fd_read;
    if (workerid2readfd[worker_id] == 0)
    {
        char file_w2c[CP_FIFO_NAME_LEN] = {0};
        sprintf(file_w2c, "%s_%d_1", CP_FIFO_NAME_PRE, worker_id); //worker 2 client
        pipe_fd_read = cpCreateFifo(file_w2c);
        if (pipe_fd_read < 0)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "pipe open Error: %s [%d]", strerror(errno), errno);
        }
        workerid2readfd[worker_id] = pipe_fd_read;
    }
    else
    {
        pipe_fd_read = workerid2readfd[worker_id];
    }
    return pipe_fd_read;
}

static void* get_attach_buf(int worker_id, int max, char *mm_name)
{
    if (semid2attbuf == NULL)
    {
        semid2attbuf = (void **) calloc(CP_GROUP_NUM*CP_GROUP_LEN, sizeof (void*));
        if (semid2attbuf == NULL)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "calloc Error: %s [%d]", strerror(errno), errno);
        }
    }
    void* buf = NULL;
    if (semid2attbuf[worker_id] == 0)
    {
        int fd = open(mm_name, O_RDWR);
        if (fd == -1)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "open error Error: %s [%d],%s", strerror(errno), errno, mm_name);
        }
        if ((buf = mmap(NULL, max, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) < 0)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "attach sys mem error Error: %s [%d]", strerror(errno), errno);
        }
        semid2attbuf[worker_id] = buf;
    }
    else
    {
        buf = semid2attbuf[worker_id];
    }

    return buf;
}

int CP_CLIENT_SERIALIZE_SEND_MEM(zval *send_data, cpClient *cli)
{
    int pipe_fd_write = get_writefd(CONN(cli)->worker_id);
    int real_len = 0;
    void* attach_addr = get_attach_buf(CONN(cli)->worker_id, CPGS->max_buffer_len, CPGS->G[CONN(cli)->group_id].workers[CONN(cli)->worker_index].sm_obj.mmap_name);

    instead_smart dest;
    dest.len = 0;
    dest.addr = attach_addr;
    dest.max = CPGS->max_buffer_len;
    dest.exceed = 0;
    php_msgpack_serialize(&dest, send_data);
    real_len = dest.len;
    if (dest.exceed == 1)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "data is exceed,increase max_read_len Error: %s [%d] ", strerror(errno), errno);
        return FAILURE;
    }

    cpWorkerInfo worker_event;
    worker_event.len = real_len;
    worker_event.pid = cpPid;
    worker_event.type = 0;
    int ret = write(pipe_fd_write, &worker_event, sizeof (worker_event));
    if (ret == -1)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "write error Error: %s [%d]", strerror(errno), errno);
    }
    return SUCCESS;

}
//core logic

static cpGroup * cpGetWorker(cpClient *cli, zval *data_source)
{
    cpGroup *G = NULL;
    int group_id, worker_index;
    for (group_id = 0; group_id < CPGS->group_num; group_id++)
    {
        if (strcmp(Z_STRVAL_P(data_source), CPGS->G[group_id].name) == 0)
        {
            G = &CPGS->G[group_id];
            cpConnection *conn = CONN(cli);
            if (cli->lock(G) == 0)
            {
                for (worker_index = 0; worker_index < G->worker_num; worker_index++)
                {
                    if (G->workers_status[worker_index] == CP_WORKER_IDLE && worker_index < G->worker_max)
                    {
                        G->workers_status[worker_index] = CP_WORKER_BUSY;
                        G->workers[worker_index].CPid = cpPid; //worker for this pid
                        conn->release = CP_FD_NRELEASED;
                        conn->worker_id = CP_WORKER_ID(group_id, worker_index);
                        conn->group_id = group_id;
                        conn->worker_index = worker_index;
                        break;
                    }
                }
                if (conn->release == CP_FD_RELEASED)
                {
                    if (G->worker_num < G->worker_max)
                    {//add
                        conn->worker_index = G->worker_num;
                        conn->release = CP_FD_NRELEASED;
                        conn->worker_id = CP_WORKER_ID(group_id, conn->worker_index);
                        conn->group_id = group_id;
                        G->workers_status[conn->worker_index] = CP_WORKER_BUSY;
                        G->workers[conn->worker_index].CPid = cpPid; //worker for this pid
                        cpCreate_worker_mem(conn->worker_index, group_id);

                        cpTcpEvent event = {0};
                        event.type = CP_TCPEVENT_ADD;
                        event.data = conn->worker_index;
                        //                         event.ClientPid = cpPid;
                        G->worker_num++; //add first, for thread safe
                        int ret = cpClient_send(cli->sock, (char *) &event, sizeof (event), 0);
                        if (ret < 0)
                        {
                            php_error_docref(NULL TSRMLS_CC, E_ERROR, "send to server errro %s [%d]", strerror(errno), errno);
                        }
                    }
                    else
                    {// in queue
                        conn->wait_fpm_pid = cpPid;
                        conn->next_wait_id = 0;
                        if (G->last_wait_id)
                        {
                            CPGS->conlist[G->last_wait_id].next_wait_id = cli->server_fd;
                            G->last_wait_id = cli->server_fd;

                        }
                        else
                        {
                            G->first_wait_id = G->last_wait_id = cli->server_fd;
                        }
                        conn->release = CP_FD_WAITING;
                        conn->group_id = group_id;
                    }
                }
                cli->unLock(G);
            }
            break;
        }
    }
    if (!G)
    {
        if (pthread_mutex_lock(&CPGS->mutex_lock) == 0)
        {
            int group_num = CPGS->group_num, group_id;
            for (group_id = 0; group_id < group_num; group_id++)
            {
                if (strcmp(Z_STRVAL_P(data_source), CPGS->G[group_id].name) == 0)
                {
                    pthread_mutex_unlock(&CPGS->mutex_lock);
                    return cpGetWorker(cli, data_source);
                }
            }

            strcpy(CPGS->G[group_num].name, Z_STRVAL_P(data_source));
            CPGS->G[group_num].worker_max = CPGS->default_max;
            CPGS->G[group_num].worker_min = CPGS->default_min;
            CPGS->G[group_num].worker_num = CPGS->default_min;
            CPGS->G[group_num].workers_status[CPGS->G[group_num].worker_num - 1] = CP_WORKER_IDLE; //foreach to CPGS->default_min to set status???
            cpCreate_worker_mem(CPGS->G[group_num].worker_num - 1, group_num); //foreach to CPGS->default_min to create mem???
            cpTcpEvent event = {0};
            event.type = CP_TCPEVENT_ADD;
            event.data = 0;
            CPGS->group_num++;
            cpClient_send(cli->sock, (char *) &event, sizeof (event), 0);

            pthread_mutex_unlock(&CPGS->mutex_lock);
        }
        return cpGetWorker(cli, data_source);
    }
    return G;
}

static CPINLINE int cli_real_send(cpClient **real_cli, zval *send_data)
{
    int ret = 0;
    cpClient *cli = *real_cli;
    if (CONN(cli)->release == CP_FD_RELEASED)
    {
        zval *data_source = NULL;
        cp_zend_hash_find(Z_ARRVAL_P(send_data), ZEND_STRS("data_source"), (void **) &data_source);
        if (manager_pid != CPGS->manager_pid)
        {//restart server
            exit(0);
        }
        cpGroup *G = cpGetWorker(cli, data_source);
        if (!G)
        {
            zend_throw_exception_ex(NULL, 0, "can not find datasource %s from pool.ini", Z_STRVAL_P(data_source) TSRMLS_CC);
            return -1;
        }
        while (CONN(cli)->release == CP_FD_WAITING)
        {
            pause();
        }
        log_start(cli);
        log_write(send_data, cli);
        ret = CP_CLIENT_SERIALIZE_SEND_MEM(send_data, cli);
    }
    else
    {
        log_write(send_data, cli);
        ret = CP_CLIENT_SERIALIZE_SEND_MEM(send_data, cli);
    }
    return ret;
}

static CPINLINE int cli_real_recv(cpClient *cli, int async)
{
    cpWorkerInfo event;
    int ret = 0;
    zval *ret_value;
    CP_ALLOC_INIT_ZVAL(ret_value);
    int pipe_fd_read = get_readfd(CONN(cli)->worker_id);
    if (async)
    {
        RecvData.type = CP_SIGEVENT_TURE;
        ZVAL_LONG(ret_value, pipe_fd_read);
        RecvData.ret_value = ret_value;
        cli->querying = 1;
        return SUCCESS;
    }
    do
    {
        ret = cpFifoRead(pipe_fd_read, &event, sizeof (event));
        if (ret < 0)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "fifo read Error: %s [%d]", strerror(errno), errno);
        }

        if (event.type == CP_SIGEVENT_DIE)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "you send the request to pool worker ,but the worker is died");
            exit(0);
        }

    } while (event.pid != cpPid); //有可能有脏数据  读出来

    log_increase_size(event.len, cli);
    void * buf = get_attach_buf(CONN(cli)->worker_id, CPGS->max_buffer_len, CPGS->G[CONN(cli)->group_id].workers[CONN(cli)->worker_index].sm_obj.mmap_name);

    php_msgpack_unserialize(ret_value, buf, event.len);

    RecvData.type = event.type;
    RecvData.ret_value = ret_value;
    return SUCCESS;
}

static void async_done(zval *object, zend_class_entry *class_entry_ptr)
{
    zval *zres;
    cpClient *cli = NULL;
    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("cli"), (void **) &zres) == SUCCESS)
    {
        CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
        if (!cli->querying)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "you must execute the async function before done");
        }
    }
    else
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "can not find zres");
    }
    zend_update_property_bool(class_entry_ptr, object, ZEND_STRL("async"), 0 TSRMLS_CC);
    cli->querying = 0;
    cpSetIsBlock(get_readfd(CONN(cli)->worker_id), 1);
    cli_real_recv(cli, 0);
}

static void check_need_exchange(zval * object, char *cur_type)
{
    char *lt = NULL;
    zval *last_type;
    // compare with last_type
    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("last_type"), (void **) &last_type) == SUCCESS)
    {
        lt = Z_STRVAL_P(last_type);
    }
    // exchange 
    if (cur_type != lt)
    {
        zend_update_property_string(pdo_connect_pool_class_entry_ptr, object, ZEND_STRL("last_type"), cur_type TSRMLS_CC);
    }
}

static char* php_check_ms(char *cmd, zval *z_args, zval* object)
{
    zval *enable_slave = NULL, *sql = NULL, *in_tran = NULL;
    char *cur_type = "m";
    if (strcasecmp("beginTransaction", cmd) == 0)
    {
        zend_update_property_bool(pdo_connect_pool_class_entry_ptr, object, ZEND_STRL("in_tran"), 1 TSRMLS_CC);
    }
    if (strcasecmp("commit", cmd) == 0 || strcasecmp("rollback", cmd) == 0)
    {
        zend_update_property_bool(pdo_connect_pool_class_entry_ptr, object, ZEND_STRL("in_tran"), 0 TSRMLS_CC);
    }
    cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("enable_slave"), (void **) &enable_slave);
    cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("in_tran"), (void **) &in_tran);
    if (!Z_BVAL_P(enable_slave) || Z_BVAL_P(in_tran))
    {//todo 
        return cur_type;
    }

    if (strcasecmp("query", cmd) == 0 || strcasecmp("exec", cmd) == 0 || strcasecmp("prepare", cmd) == 0)
    {
        cp_zend_hash_index_find(Z_ARRVAL_P(z_args), 0, (void**) &sql);
        cp_convert_to_string_ex(&sql);
        char pre[8] = {0};
        strncpy(pre, Z_STRVAL_P(sql), 6);
        int i = 0;
        for (; i < 6; i++)
        {
            if (pre[i] == ' ')
            {
                pre[i] = '\0';
                break;
            }
            pre[i] = tolower(pre[i]);
        }
        if (strcmp(pre, "select") == 0 || strcmp(pre, "show") == 0)
        {
            cur_type = "s";
        }
        else
        {
            cur_type = "m";
        }
    }
    return cur_type;
}

int cp_system_random(int min, int max)
{
    char *next_random_byte;
    int bytes_to_read;
    unsigned random_value;

    assert(max > min);

    if (dev_random_fd == -1)
    {
        dev_random_fd = open("/dev/urandom", O_RDONLY);
        assert(dev_random_fd != -1);
    }

    next_random_byte = (char *) &random_value;
    bytes_to_read = sizeof (random_value);

    if (read(dev_random_fd, next_random_byte, bytes_to_read) < 0)
    {
        return -1;
    }
    return min + (random_value % (max - min + 1));
}


//create the pass args that pass to mysql

static CPINLINE zval* create_pass_data(char* cmd, zval* z_args, zval* object, char* cur_type, zval **ret_data_source)
{
    zval *data_source = NULL, *username = NULL, *pwd = NULL, *options = NULL, *pass_data = NULL, *zval_conf = NULL, *real_data_source_arr = NULL;
    zval_conf = cp_zend_read_property(pdo_connect_pool_class_entry_ptr, object, ZEND_STRL("config"), 0 TSRMLS_DC);

    zval *slave;
    if (*cur_type == 's' && (cp_zend_hash_find(Z_ARRVAL_P(zval_conf), ZEND_STRS("slave"), (void **) &slave) == SUCCESS))
    {
        zval *start_prt, start, end, *end_prt;
        start_prt = &start;
        end_prt = &end;

        cp_zend_hash_find(Z_ARRVAL_P(zval_conf), ZEND_STRS("slave"), (void **) &slave);
        int slave_cnt;
        slave_cnt = zend_hash_num_elements(Z_ARRVAL_P(slave));

        if (slave_cnt > 0)
        {
            int index;
            index = cp_system_random(0, (slave_cnt - 1));

            if (cp_zend_hash_index_find(Z_ARRVAL_P(slave), index, (void **) &real_data_source_arr) != SUCCESS)
            {
                php_error_docref(NULL TSRMLS_CC, E_ERROR, "not find slave ,check config");
            }
        }
    }
    else
    {
        if (cp_zend_hash_find(Z_ARRVAL_P(zval_conf), ZEND_STRS("master"), (void **) &real_data_source_arr) != SUCCESS)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "not find master ,check config");
        }
    }
    // find args
    cp_zend_hash_find(Z_ARRVAL_P(real_data_source_arr), ZEND_STRS("data_source"), (void **) &data_source);
    cp_zend_hash_find(Z_ARRVAL_P(real_data_source_arr), ZEND_STRS("username"), (void **) &username);
    cp_zend_hash_find(Z_ARRVAL_P(real_data_source_arr), ZEND_STRS("pwd"), (void **) &pwd);

    CP_MAKE_STD_ZVAL(pass_data);
    array_init(pass_data);
    cp_add_assoc_string(pass_data, "type", "pdo", 1);
    cp_add_assoc_string(pass_data, "method_type", "connect", 1);
    cp_add_assoc_string(pass_data, "method", cmd, 1);
    cp_add_assoc_string(pass_data, "data_source", Z_STRVAL_P(data_source), 1);
    *ret_data_source = data_source;
    cp_add_assoc_string(pass_data, "username", Z_STRVAL_P(username), 1);
    cp_add_assoc_string(pass_data, "password", Z_STRVAL_P(pwd), 1);
    cp_zval_add_ref(&z_args);
    add_assoc_zval(pass_data, "args", z_args);
    if (cp_zend_hash_find(Z_ARRVAL_P(real_data_source_arr), ZEND_STRS("options"), (void **) &options) != SUCCESS)
    {
        zval *new_option = NULL;
        CP_MAKE_STD_ZVAL(new_option);
        array_init(new_option);
        add_index_long(new_option, PDO_ATTR_ERRMODE, PDO_ERRMODE_EXCEPTION);
        cp_add_index_string(new_option, PDO_ATTR_DRIVER_SPECIFIC + 2, "SET SESSION wait_timeout=31536000", 1);
        add_assoc_zval(pass_data, "options", new_option);
    }
    else
    {
        cp_zval_add_ref(&options);
        add_index_long(options, PDO_ATTR_ERRMODE, PDO_ERRMODE_EXCEPTION); //set exception mode for delete pdo object from pool when gone away
        cp_add_index_string(options, PDO_ATTR_DRIVER_SPECIFIC + 2, "SET SESSION wait_timeout=31536000", 1);
        add_assoc_zval(pass_data, "options", options);
    }
    return pass_data;
}

int static stmt_fetch_obj(zval *args, zend_class_entry *ce, zval *return_value)
{
    zend_fcall_info fci = {0};
    zend_fcall_info_cache fcc = {0};
    fci.size = sizeof (zend_fcall_info);


    if (ce->constructor)
    {
#if PHP_MAJOR_VERSION == 7
        zval retval;
        ZVAL_UNDEF(&fci.function_name);
        fci.retval = &retval;
        fci.param_count = 0;
        fci.params = NULL;
        fci.no_separation = 1;
        fci.object = Z_OBJ_P(return_value);

        zend_fcall_info_args_ex(&fci, ce->constructor, args);

        fcc.initialized = 1;
        fcc.function_handler = ce->constructor;
        fcc.called_scope = Z_OBJCE_P(return_value);
        fcc.object = Z_OBJ_P(return_value);
#else
        zval *retval;
        fci.function_table = &ce->function_table;
        fci.function_name = NULL;
        fci.symbol_table = NULL;
        fci.retval_ptr_ptr = &retval;
        if (args)
        {
            HashTable *ht = Z_ARRVAL_P(args);
            Bucket *p;

            fci.param_count = 0;
            fci.params = safe_emalloc(sizeof (zval**), ht->nNumOfElements, 0);
            p = ht->pListHead;
            while (p != NULL)
            {
                fci.params[fci.param_count++] = (zval**) p->pData;
                p = p->pListNext;
            }
        }
        else
        {
            fci.param_count = 0;
            fci.params = NULL;
        }
        fci.no_separation = 1;

        fcc.initialized = 1;
        fcc.function_handler = ce->constructor;
        fcc.calling_scope = EG(scope);
        fcc.called_scope = ce;

        object_init_ex(return_value, ce);

        fci.object_ptr = return_value;
        fcc.object_ptr = return_value;
#endif
        if (zend_call_function(&fci, &fcc) == FAILURE)
        {
            php_error_docref(NULL TSRMLS_CC, E_ERROR, "could not call class constructor");
            return 0;
        }
        else
        {//??? free something?
#if PHP_MAJOR_VERSION < 7
            efree(fci.params);
            cp_zval_ptr_dtor(&retval);
#endif
            //cp_zval_ptr_dtor(&args);
            return 1;
        }
    }
    else
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "user-supplied class does not have a constructor");
        return 0;
    }
}

PHP_METHOD(pdo_connect_pool_PDOStatement, __call)
{
    zval *zres, *source_zval, *pass_data, *object, *z_args, *class_name = NULL, stmt_obj_args_dup, *async_zval;
    char *cmd;
    zend_size_t cmd_len;
    int async = 0;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Osa", &object, pdo_connect_pool_PDOStatement_class_entry_ptr, &cmd, &cmd_len, &z_args) == FAILURE)
    {
        RETURN_FALSE;
    }
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("async"), (void **) &async_zval) == SUCCESS)
    {
        async = Z_BVAL_P(async_zval);
    }
    cpClient *cli;
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("cli"), (void **) &zres) == SUCCESS)
    {
        CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
    }
    else
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "pdo_connect_pool: object is not instanceof pdo_connect_pool. ");
        RETURN_FALSE;
    }
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("data_source"), (void **) &source_zval) == FAILURE)
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "pdo_connect_pool: get data_source name failed!");
        RETURN_FALSE;
    }

    CP_MAKE_STD_ZVAL(pass_data);
    array_init(pass_data);
    cp_add_assoc_string(pass_data, "data_source", Z_STRVAL_P(source_zval), 1);
    cp_add_assoc_string(pass_data, "method", cmd, 1);
    cp_zval_add_ref(&z_args);
    add_assoc_zval(pass_data, "args", z_args);
    if (strcasecmp("fetchObject", cmd) == 0)
    {
        cp_zend_hash_del(Z_ARRVAL_P(pass_data), "args", strlen("args") + 1); //use no args in the pool server
        cp_zend_hash_index_find(Z_ARRVAL_P(z_args), 0, (void**) &class_name);
        if (class_name)
        {
            ZVAL_DUP(&stmt_obj_args_dup, z_args);
            zend_hash_index_del(Z_ARRVAL_P(z_args), 0);
        }
    }
    cp_add_assoc_string(pass_data, "method_type", "PDOStatement", 1);
    cp_add_assoc_string(pass_data, "type", "pdo", 1);
    int ret = cli_real_send(&cli, pass_data);
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "cli_real_send faild error Error: %s [%d] ", strerror(errno), errno);
    }
    cli_real_recv(cli, async);
    if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else if (RecvData.type == CP_SIGEVENT_STMT_OBJ)
    {//fetchObject return
        if (class_name && strcasecmp("stdClass", Z_STRVAL_P(class_name)) != 0)
        {//user class
            zend_class_entry *class_ce = cp_zend_fetch_class(class_name, ZEND_FETCH_CLASS_AUTO);
            stmt_fetch_obj(&stmt_obj_args_dup, class_ce, return_value);
            zval *val;
            char *name;
            uint klen;
            int ktype;

            CP_HASHTABLE_FOREACH_START2(CP_Z_ARRVAL_P(RecvData.ret_value), name, klen, ktype, val)
            {
                zend_update_property(class_ce, return_value, name, klen, val TSRMLS_CC);
            }
            CP_HASHTABLE_FOREACH_END();
        }
        else
        {//default class
            convert_to_object(RecvData.ret_value);
#if PHP_MAJOR_VERSION == 7
            ZVAL_OBJ(return_value, Z_OBJ_P(RecvData.ret_value));

#else
            ZVAL_DUP(return_value, RecvData.ret_value);
#endif

        }
    }
    else
    {

        RETVAL_ZVAL(RecvData.ret_value, 0, 1);
    }
    cp_zval_ptr_dtor(&pass_data);
}

PHP_METHOD(pdo_connect_pool, __call)
{
    zval *z_args, *pass_data, *object, *zres, *source_zval, *use_ms, *async_zval;
    char *cmd, *cur_type;
    zend_size_t cmd_len;
    int async = 0;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Osa", &object, pdo_connect_pool_class_entry_ptr, &cmd, &cmd_len, &z_args) == FAILURE)
    {
        RETURN_FALSE;
    }

    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("async"), (void **) &async_zval) == SUCCESS)
    {
        async = Z_BVAL_P(async_zval);
    }
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("use_ms"), (void **) &use_ms) == SUCCESS)
    {
        cur_type = "m";
    }
    else
    {
        cur_type = php_check_ms(cmd, z_args, object);
        check_need_exchange(getThis(), cur_type);
    }
    pass_data = create_pass_data(cmd, z_args, object, cur_type, &source_zval);
    cpClient *cli;
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("cli"), (void **) &zres) == SUCCESS)
    {
        CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
    }
    else
    {
        zres = cpConnect_pool_server(source_zval, async);
        zend_update_property_string(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("data_source"), Z_STRVAL_P(source_zval) TSRMLS_CC);
        zend_update_property(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("cli"), zres TSRMLS_CC);
        CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
        if (async)
        {
            cli->async = async;
            cli->dummy_source_index = dummy_source_index - 1;
        }
    }
    if (async && cli->querying)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "the obj is async querying now, you can not execute async function again");
    }
    int ret = cli_real_send(&cli, pass_data);
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "cli_real_send faild error Error: %s [%d] ", strerror(errno), errno);
    }
    cli_real_recv(cli, async);
    if (RecvData.type == CP_SIGEVENT_PDO)
    {
        object_init_ex(return_value, pdo_connect_pool_PDOStatement_class_entry_ptr);
        zend_update_property(pdo_connect_pool_PDOStatement_class_entry_ptr, return_value, ZEND_STRL("cli"), zres TSRMLS_CC);
        zend_update_property(pdo_connect_pool_PDOStatement_class_entry_ptr, return_value, ZEND_STRL("data_source"), source_zval TSRMLS_CC); //标示这个连接的真实目标
        zval *query_string;
        if (cp_zend_hash_index_find(Z_ARRVAL_P(z_args), 0, (void**) &query_string) == SUCCESS) {
            zend_update_property(pdo_connect_pool_PDOStatement_class_entry_ptr, return_value, ZEND_STRL("queryString"), query_string TSRMLS_CC);
        }
        cp_zval_ptr_dtor(&RecvData.ret_value);
    }
    else if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else
    {
        RETVAL_ZVAL(RecvData.ret_value, 0, 1);
    }
    cp_zval_ptr_dtor(&pass_data);
}

PHP_METHOD(pdo_connect_pool, __destruct)
{
}

PHP_METHOD(pdo_connect_pool, __construct)
{
    zval *zval_conf = NULL, *data_source = NULL, *options = NULL, *master = NULL;
    zval *object = getThis();
    char *username = NULL, *password = NULL;
    zend_size_t usernamelen, passwordlen;
    int port = CP_PORT_PDO;

    if (FAILURE == zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "z|s!s!a!l!", &data_source,
            &username, &usernamelen, &password, &passwordlen, &options, &port))
    {
        ZVAL_NULL(object);
        return;
    }
    CP_GET_PID;
    switch (Z_TYPE_P(data_source))
    {
        case IS_STRING:
        {
            CP_MAKE_STD_ZVAL(zval_conf);
            CP_MAKE_STD_ZVAL(master);

            array_init(zval_conf);
            array_init(master);

            cp_add_assoc_string(master, "data_source", Z_STRVAL_P(data_source), 1);
            cp_add_assoc_string(master, "username", username, 1);
            cp_add_assoc_string(master, "pwd", password, 1);
            if (options != NULL)
            {
                cp_zval_add_ref(&options);
                add_assoc_zval(master, "options", options);
            }

            add_assoc_zval(zval_conf, "master", master);
            // zend_print_zval_r(zval_conf, 0);

            zend_update_property_bool(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("use_ms"), 0 TSRMLS_CC);
            zend_update_property(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("config"), zval_conf TSRMLS_CC);
            cp_zval_ptr_dtor(&zval_conf);
        }
            break;
        case IS_ARRAY:
            cp_zval_add_ref(&data_source);
            zend_update_property(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("config"), data_source TSRMLS_CC);

            break;
    }
    zend_update_property_bool(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("in_tran"), 0 TSRMLS_CC);
    zend_update_property_bool(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("enable_slave"), 1 TSRMLS_CC);
}

PHP_METHOD(pdo_connect_pool, release)
{
    release_worker(getThis());
    CP_CHECK_RETURN(1);
}

PHP_METHOD(pdo_connect_pool_PDOStatement, release)
{
    release_worker(getThis());
    CP_CHECK_RETURN(1);
}

PHP_METHOD(pdo_connect_pool, close)
{
    close_conn(getThis());
    CP_CHECK_RETURN(1);
}

PHP_METHOD(pdo_connect_pool, setAsync)
{
    int async = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l!", &async) == FAILURE)
    {
        RETURN_FALSE;
    }
    zend_update_property_bool(pdo_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("async"), async TSRMLS_CC);
    CP_CHECK_RETURN(1);
}

PHP_METHOD(pdo_connect_pool_PDOStatement, setAsync)
{
    int async = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l!", &async) == FAILURE)
    {
        RETURN_FALSE;
    }
    zend_update_property_bool(pdo_connect_pool_PDOStatement_class_entry_ptr, getThis(), ZEND_STRL("async"), async TSRMLS_CC);
    CP_CHECK_RETURN(1);
}

PHP_METHOD(redis_connect_pool, release)
{

    release_worker(getThis());
    CP_CHECK_RETURN(1);
}

PHP_METHOD(redis_connect_pool, close)
{
    close_conn(getThis());
    CP_CHECK_RETURN(1);
}

PHP_METHOD(redis_connect_pool, setAsync)
{
    int async = 0;
    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l!", &async) == FAILURE)
    {
        RETURN_FALSE;
    }
    zend_update_property_bool(redis_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("async"), async TSRMLS_CC);
    CP_CHECK_RETURN(1);
}

PHP_METHOD(redis_connect_pool, __destruct)
{
}

PHP_METHOD(redis_connect_pool, __construct)
{
    CP_GET_PID;
}

PHP_METHOD(redis_connect_pool, connect)
{
    char *ip;
    zend_size_t ip_len;
    char *port = "6379";
    zend_size_t port_len = 0;
    char *time;
    zend_size_t time_len = 0;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s!|s!s!", &ip, &ip_len, &port, &port_len, &time, &time_len) == FAILURE)
    {

        RETURN_FALSE;
    }
    //临时标示这个连接的真实目标,根据下一步是select还是get来确定db号,可以減少一次select操作
    zend_update_property_string(redis_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("ip"), ip TSRMLS_CC);
    zend_update_property_string(redis_connect_pool_class_entry_ptr, getThis(), ZEND_STRL("port"), port TSRMLS_CC);

    RETURN_TRUE;
}


static cpClient * cpRedis_conn_pool_server(zval *obj, char *source_char, int async)
{
    zval *zres;
    cpClient *cli;
    if (cp_zend_hash_find(Z_OBJPROP_P(obj), ZEND_STRS("cli"), (void **) &zres) == SUCCESS)
    {
        CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
    }
    else
    {//connect local proxy

        zval z_data_source;
        CP_ZVAL_STRING(&z_data_source, source_char, 0);
        zval *tmp = cpConnect_pool_server(&z_data_source, async);
        CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &tmp, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
        if (async)
        {//todo check cannot exchange sync/async
            cli->async = async;
            cli->dummy_source_index = dummy_source_index - 1;
        }

        zend_update_property(redis_connect_pool_class_entry_ptr, obj, ZEND_STRL("cli"), tmp TSRMLS_CC);
    }
    return cli;
}

PHP_METHOD(redis_connect_pool, auth)
{
    char *auth;
    zend_size_t auth_len;
    zval *object;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Os!", &object, redis_connect_pool_class_entry_ptr, &auth, &auth_len) == FAILURE)
    {
        RETURN_FALSE;
    }

    zend_update_property_string(redis_connect_pool_class_entry_ptr, object, ZEND_STRL("auth"), auth TSRMLS_CC);

}

PHP_METHOD(redis_connect_pool, select)
{
    zval *ip, *port, *z_args, *pass_data, *object, *auth;
    char source_char[CP_SOURCE_MAX] = {0};
    char *db;
    zend_size_t db_len;
    cpClient *cli;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Os!", &object, redis_connect_pool_class_entry_ptr, &db, &db_len) == FAILURE)
    {
        RETURN_FALSE;
    }

    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("ip"), (void **) &ip) == SUCCESS)
    {
        strcat(source_char, Z_STRVAL_P(ip));
        strcat(source_char, ":");
    }
    else
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "redis_connect_pool: IP is empty ");
        RETURN_FALSE;
    }

    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("port"), (void **) &port) == SUCCESS)
    {
        strcat(source_char, Z_STRVAL_P(port));
        strcat(source_char, ":");
    }
    else
    {
        php_error_docref(NULL TSRMLS_CC, E_WARNING, "redis_connect_pool: PORT is empty");
        RETURN_FALSE;
    }

    zend_update_property_string(redis_connect_pool_class_entry_ptr, object, ZEND_STRL("db"), db TSRMLS_CC);
    strcat(source_char, db);
    zend_update_property_string(redis_connect_pool_class_entry_ptr, object, ZEND_STRL("data_source"), source_char TSRMLS_CC); //确定数据源

    cli = cpRedis_conn_pool_server(getThis(), source_char, 0);

    CP_MAKE_STD_ZVAL(pass_data);
    array_init(pass_data);
    cp_add_assoc_string(pass_data, "type", "redis", 1);
    cp_add_assoc_string(pass_data, "method", "select", 1);
    cp_add_assoc_string(pass_data, "data_source", source_char, 1);
    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("auth"), (void **) &auth) == SUCCESS)
    {
        cp_add_assoc_string(pass_data, "auth", Z_STRVAL_P(auth), 1);
    }

    CP_MAKE_STD_ZVAL(z_args);
    array_init(z_args);
    cp_add_assoc_string(z_args, "db", db, 1);
    add_assoc_zval(pass_data, "args", z_args);

    int ret = cli_real_send(&cli, pass_data);
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "cli_real_send faild error Error: %s [%d] ", strerror(errno), errno);
    }

    cli_real_recv(cli, 0);
    if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else
    {

        RETVAL_ZVAL(RecvData.ret_value, 0, 1);
    }
    cp_zval_ptr_dtor(&pass_data);
}

PHP_METHOD(redis_connect_pool, done)
{
    async_done(getThis(), redis_connect_pool_class_entry_ptr);
    if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else
    {
        RETVAL_ZVAL(RecvData.ret_value, 0, 1); //no copy  destroy
    }
}

PHP_METHOD(pdo_connect_pool, done)
{
    async_done(getThis(), pdo_connect_pool_class_entry_ptr);
    if (RecvData.type == CP_SIGEVENT_PDO)
    {
        zval *zres, *source_zval;
        cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("cli"), (void **) &zres);
        cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("data_source"), (void **) &source_zval);
        object_init_ex(return_value, pdo_connect_pool_PDOStatement_class_entry_ptr);
        zend_update_property(pdo_connect_pool_PDOStatement_class_entry_ptr, return_value, ZEND_STRL("cli"), zres TSRMLS_CC);
        zend_update_property(pdo_connect_pool_PDOStatement_class_entry_ptr, return_value, ZEND_STRL("data_source"), source_zval TSRMLS_CC); //标示这个连接的真实目标
        cp_zval_ptr_dtor(&RecvData.ret_value);
    }
    else if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else
    {
        RETVAL_ZVAL(RecvData.ret_value, 0, 1);
    }
}

PHP_METHOD(pdo_connect_pool_PDOStatement, done)
{
    async_done(getThis(), pdo_connect_pool_PDOStatement_class_entry_ptr);
    if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else if (RecvData.type == CP_SIGEVENT_STMT_OBJ)
    {//fetchObject return !!!!!cos the user class name ,now just support default class 
        convert_to_object(RecvData.ret_value);
#if PHP_MAJOR_VERSION == 7
        ZVAL_OBJ(return_value, Z_OBJ_P(RecvData.ret_value));
#else
        ZVAL_DUP(return_value, RecvData.ret_value);
#endif
    }
    else
    {
        RETVAL_ZVAL(RecvData.ret_value, 0, 1);
    }
}

PHP_METHOD(redis_connect_pool, __call)
{
    zval *z_args, *object, *zres, *source_zval, *pass_data, *async_zval, *auth;
    char source_char[CP_SOURCE_MAX] = {0};
    char *cmd;
    zend_size_t cmd_len;
    cpClient *cli;
    int async = 0;

    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Osa", &object, redis_connect_pool_class_entry_ptr, &cmd, &cmd_len, &z_args) == FAILURE)
    {
        RETURN_FALSE;
    }
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("async"), (void **) &async_zval) == SUCCESS)
    {
        async = Z_BVAL_P(async_zval);
    }
    cp_zval_add_ref(&z_args);
    CP_MAKE_STD_ZVAL(pass_data);
    array_init(pass_data);
    cp_add_assoc_string(pass_data, "method", cmd, 1);
    cp_add_assoc_string(pass_data, "type", "redis", 1);
    if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("auth"), (void **) &auth) == SUCCESS)
    {
        cp_add_assoc_string(pass_data, "auth", Z_STRVAL_P(auth), 1);
    }
    add_assoc_zval(pass_data, "args", z_args);
    if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("data_source"), (void **) &source_zval) == SUCCESS)
    {
        cp_add_assoc_string(pass_data, "data_source", Z_STRVAL_P(source_zval), 1);
        if (cp_zend_hash_find(Z_OBJPROP_P(getThis()), ZEND_STRS("cli"), (void **) &zres) == SUCCESS)
        {
            CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);
        }
        else
        {
            php_error_docref(NULL TSRMLS_CC, E_WARNING, "redis_connect_pool: object is not instanceof redis_connect_pool. ");
            RETURN_FALSE;
        }
    }
    else
    {
        zval *ip, *port;
        if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("ip"), (void **) &ip) == SUCCESS)
        {
            strcat(source_char, Z_STRVAL_P(ip));
            strcat(source_char, ":");
        }
        else
        {
            php_error_docref(NULL TSRMLS_CC, E_WARNING, "redis_connect_pool: IP is empty ");
            RETURN_FALSE;
        }
        if (cp_zend_hash_find(Z_OBJPROP_P(object), ZEND_STRS("port"), (void **) &port) == SUCCESS)
        {
            strcat(source_char, Z_STRVAL_P(port));
            strcat(source_char, ":");
        }
        else
        {
            php_error_docref(NULL TSRMLS_CC, E_WARNING, "redis_connect_pool: PORT is empty");
            RETURN_FALSE;
        }
        strcat(source_char, "0");
        zend_update_property_string(redis_connect_pool_class_entry_ptr, object, ZEND_STRL("data_source"), source_char TSRMLS_CC); //确定数据源
        cp_add_assoc_string(pass_data, "data_source", source_char, 1);
        cli = cpRedis_conn_pool_server(getThis(), source_char, async);
    }
    if (async && cli->querying)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "the obj is async querying now, you can not execute async function again");
    }
    int ret = cli_real_send(&cli, pass_data);
    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "cli_real_send faild error Error: %s [%d] ", strerror(errno), errno);
    }

    cli_real_recv(cli, async);
    if (RecvData.type == CP_SIGEVENT_EXCEPTION)
    {
        release_worker(getThis());
        zend_throw_exception(NULL, Z_STRVAL_P(RecvData.ret_value), 0 TSRMLS_CC);
        RETVAL_BOOL(0);
    }
    else
    {
        RETVAL_ZVAL(RecvData.ret_value, 0, 1); //no copy  destroy
    }
    cp_zval_ptr_dtor(&pass_data);
}

PHP_METHOD(memcached_connect_pool, __construct)
{
    CP_GET_PID;
}

PHP_METHOD(memcached_connect_pool, __destruct)
{
}

PHP_METHOD(memcached_connect_pool, __call)
{
    zval *object, *z_args, *pass_data, *zres, data_source;
    char *cmd;
    zend_size_t cmd_len;
    char *data_source_string = "127.0.0.1:11211";
    int async = 0;
    cpClient *cli;

    if (zend_parse_method_parameters(
                ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Os!a!",
                &object, memcached_connect_pool_class_entry_ptr,
                &cmd, &cmd_len, &z_args) == FAILURE)
        RETURN_FALSE;

    CP_ZVAL_STRING(&data_source, data_source_string, 0);
    cp_zval_add_ref(&z_args);


    CP_MAKE_STD_ZVAL(pass_data);
    array_init(pass_data);
    cp_add_assoc_string(pass_data, "data_source", data_source_string, 1);
    cp_add_assoc_string(pass_data, "type", "memcached", 1);
    cp_add_assoc_string(pass_data, "method", cmd, 1);
    add_assoc_zval(pass_data, "args", z_args);

    zres = cpConnect_pool_server(&data_source, async);
    CP_ZEND_FETCH_RESOURCE_NO_RETURN(cli, cpClient*, &zres, -1, CP_RES_CLIENT_NAME, le_cli_connect_pool);

    int ret = cli_real_send(&cli, pass_data);

    if (ret < 0)
    {
        php_error_docref(NULL TSRMLS_CC, E_ERROR, "cli_real_send faild error Error: %s [%d] ", strerror(errno), errno);
    }

    cli_real_recv(cli, async);

    RETVAL_ZVAL(RecvData.ret_value, 0, 1);
    cp_zval_ptr_dtor(&pass_data);
}

PHP_FUNCTION(pool_server_status)
{
    long pid;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "l", &pid) == FAILURE || pid == 0)
    {
        php_printf("server is not running. \n");
        return;
    }

    if (kill(pid, 0) < 0)
    {
        php_printf("server with pid %d has run away unexpectedly. \n", pid);
        return;
    }
    else
    {
        cpClient_attach_mem();
        php_printf("\nserver with pid %d is running well. \n", pid);

        cpGroup *G = NULL;
        int group_id, queue_num;
        php_printf("\ngroup number: %d\n", CPGS->group_num);
        for (group_id = 0; group_id < CPGS->group_num; group_id++)
        {
            queue_num = 0;
            G = &CPGS->G[group_id];
            php_printf("\ngroup %d: %s\n", group_id, G->name);

            int current_fd = G->first_wait_id;
            while (current_fd)
            {
                cpConnection* conn = &(CPGS->conlist[current_fd]);
                current_fd = conn->next_wait_id;
                queue_num++;
            }

            php_printf("have used %d,the max conn num is %d, the min num is %d,the queue len is %d\n", G->worker_num, G->worker_max, G->worker_min, queue_num);

        }

    }


}

