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
#include <signal.h>
#include <sys/wait.h>
zval *pdo_object = NULL;
zval *redis_object = NULL;
zval *memcached_object = NULL;

static void cpWorker_do_stop()
{
    cpGroup *G = &CPGS->G[CPWG.gid];
    if (G->workers[CPWG.id].pid != CPWG.pid && CPWG.event.pid != 0)//G->workers[CPWG.id].pid IS 0 or new pid
    {//i am die already
        int ret = write(CPWG.pipe_fd_read, &CPWG.event, sizeof (cpWorkerInfo)); //write back give the real worker
        if (ret < 0)
        {
            cpLog("fifo read Error: %s [%d]", strerror(errno), errno);
        }
    }
    exit(0);
}

static void cpWorker_init(int worker_id, int group_id)
{
    //标识为worker进程
    CPGL.process_type = CP_PROCESS_WORKER;
    cpWorker_attach_mem(worker_id, group_id);

    CPWG.id = worker_id;
    CPWG.gid = group_id;
    CPWG.pid = getpid();

    char fifo_name[CP_FIFO_NAME_LEN] = {0};
    sprintf(fifo_name, "%s_%d", CP_FIFO_NAME_PRE, CP_WORKER_ID(group_id, worker_id)); //client 2 worker
    CPWG.pipe_fd_read = cpCreateFifo(fifo_name);

    sprintf(fifo_name, "%s_%d_1", CP_FIFO_NAME_PRE, CP_WORKER_ID(group_id, worker_id)); //worker 2 client
    int pipe_fd_write = cpCreateFifo(fifo_name);
    CPWG.pipe_fd_write = pipe_fd_write;
}

static int cpWorker_loop(int worker_id, int group_id)
{
    cpWorker_init(worker_id, group_id);
    cpGroup *G = &CPGS->G[group_id];
    cpShareMemory *sm_obj = &(G->workers[worker_id].sm_obj);
    int ret;
    cpSettitle(G->name);
    cpSignalSet(SIGALRM, cpWorker_do_ping, 1, 0);
    cpSignalSet(SIGTERM, cpWorker_do_stop, 1, 0);
    alarm(CPGC.ping_time);
    //    zval *ret_value;
    //    CP_ALLOC_INIT_ZVAL(ret_value);
    while (CPGS->running)
    {
        zval *ret_value;
        CP_ALLOC_INIT_ZVAL(ret_value);
        bzero(&CPWG.event, sizeof (cpWorkerInfo));
        ret = cpFifoRead(CPWG.pipe_fd_read, &CPWG.event, sizeof (cpWorkerInfo));
        if (ret < 0)
        {
            cpLog("fifo read Error: %s [%d]", strerror(errno), errno);
        }
        if (CPWG.event.pid != G->workers[CPWG.id].CPid)
        {//pipe数据里面的fpm pid和worker应该服务的pid不一样
            cpLog("warning: read a wrong event,maybe you restart the pool server,%d,%d,%d", CPWG.event.pid, CPWG.id, G->workers[CPWG.id].CPid);
            continue;
        }
        CPWG.working = 1;

        php_msgpack_unserialize(ret_value, sm_obj->mem, CPWG.event.len);

        worker_onReceive(ret_value);
        CPWG.working = 0;
    }
    return SUCCESS;
}

int cpFork_one_worker(int worker_id, int group_id)
{
    int pid, ret;
    pid = fork();
    if (pid < 0)
    {
        cpLog("Fork Worker failed. Error: %s [%d]", strerror(errno), errno);
        return FAILURE;
    }
    else if (pid == 0)
    {
        ret = cpWorker_loop(worker_id, group_id);
        exit(ret);
    }
    else
    {
        return pid;
    }
}

static void cpManagerRecycle(int sig)
{
    int i, recycle_num, j;
    //    cpLog("monitor:start___________________");
    for (j = 0; j < CPGS->group_num; j++)
    {
        cpGroup *G = &CPGS->G[j];
        recycle_num = 0;
        //        cpLog("monitor:the  '%s' have used %d,the max conn num is %d, the min num is %d", G->name, G->worker_num, G->worker_max, G->worker_min);
        if (G->lock(G) == 0)
        {
            //                                    for (i = G->worker_num - 1; i >= 0; i--)
            //                                    {
            //                                        cpLog("index is %d,pid is %d,status is %d", i, G->workers[i].pid, G->workers_status[i]);
            //                                    }
            //                                    cpLog("________________");

            for (i = G->worker_num - 1; i >= G->worker_min; i--)
            {
                if (G->workers_status[i] == CP_WORKER_BUSY)
                {//已经busy了就退出,否则会有跳号bug
                    break;
                }
                if (G->workers[i].pid == 0)
                {//争抢的时候就++了 所以会出现0的情况
                    continue;
                }
                if (G->workers_status[i] == CP_WORKER_IDLE)
                {//当前worker数大于最小 并且空闲
                    int ret = kill(G->workers[i].pid, SIGTERM);
                    if (ret == -1)
                    {
                        cpLog("[Manager]kill failed, id=%d. Error: %s [%d]", i, strerror(errno), errno);
                    }
                    else
                    {
                        G->worker_num--;
                        G->workers_status[i] = CP_WORKER_DEL;
                        G->workers[i].pid = 0;
                        cpShareMemory *sm_obj = &(G->workers[i].sm_obj);
                        sm_obj->mem = NULL;
                        if (++recycle_num >= CPGC.recycle_num)
                        {
                            break; //一个一个回收
                        }
                    }
                }
            }
            G->unLock(G);
        }
    }
    //    cpLog("monitor:end___________________\n");
    alarm(CPGC.idel_time);
}

static void cpManagerAdd(int sig)
{
    int i, j;

    for (j = 0; j < CPGS->group_num; j++)
    {
        cpGroup *G = &CPGS->G[j];
        for (i = G->worker_num - 1; i >= 0; i--)//for not set source in pool.ini
            //for (i = G->worker_num - 1; i >= G->worker_min; i--)
        {
            if (G->workers[i].pid == 0)
            {//只创建刚分配并且pid为0的
                int new_pid = cpFork_one_worker(i, j);
                if (new_pid < 0)
                {
                    cpLog("Fork worker process failed. Error: %s [%d]", strerror(errno), errno);
                }
                else
                {
                    G->workers[i].pid = new_pid;
                }
            }
        }
    }

}

static void cpManagerReload(int sig)
{
    zval *group_conf = NULL, *v;
    group_conf = cpGetConfig(CPGC.ini_file);
    int gid = 0;
    zval *gid_ptr = NULL;
    cpGroup *G = NULL;
    if (!Z_BVAL_P(group_conf))
    {
        cpLog("parse ini file[%s]  reload error!", CPGC.ini_file);
    }
    else
    {
        zval *config;
        char *name;
        int keytype;
        uint32_t keylen;

        CP_HASHTABLE_FOREACH_START2(CP_Z_ARRVAL_P(group_conf), name, keylen, keytype, config)
        {
            if (strcmp(name, "common") != 0)
            {
                if (cp_zend_hash_find(Z_ARRVAL_P(CPGS->group), name, strlen(name) + 1, (void **) &gid_ptr) == SUCCESS)
                {
                    gid = Z_LVAL_P(gid_ptr);
                    G = &CPGS->G[gid];
                }
                else
                {
                    cpLog("can not add datasource when the server runing,if you want add it please restart");
                    return;
                }
                if (G->lock(G) == 0)
                {
                    if (cp_zend_hash_find(Z_ARRVAL_P(config), ZEND_STRS("pool_max"), (void **) &v) == SUCCESS)
                    {
                        convert_to_long(v);
                        G->worker_max = (int) Z_LVAL_P(v);
                    }
                    if (cp_zend_hash_find(Z_ARRVAL_P(config), ZEND_STRS("pool_min"), (void **) &v) == SUCCESS)
                    {
                        convert_to_long(v);
                        int new_min = (int) Z_LVAL_P(v);
                        if (new_min > G->worker_min)
                        {//增加最小
                            while (G->worker_num < new_min)
                            {
                                cpCreate_worker_mem(G->worker_num, gid);
                                G->workers_status[G->worker_num] = CP_WORKER_IDLE;
                                G->worker_num++; //先加 线程安全
                                int new_pid = cpFork_one_worker(G->worker_num - 1, gid);
                                if (new_pid < 0)
                                {
                                    cpLog("Fork worker process failed. Error: %s [%d]", strerror(errno), errno);
                                }
                                else
                                {
                                    G->workers[G->worker_num - 1].pid = new_pid;
                                }
                            }
                        }
                        G->worker_min = new_min;
                    }

                    G->unLock(G);
                }
            }
            else
            {
                if (cp_zend_hash_find(Z_ARRVAL_P(config), ZEND_STRS("recycle_num"), (void **) &v) == SUCCESS)
                {
                    convert_to_long(v);
                    CPGC.recycle_num = (int) Z_LVAL_P(v);
                }
                if (cp_zend_hash_find(Z_ARRVAL_P(config), ZEND_STRS("idel_time"), (void **) &v) == SUCCESS)
                {
                    convert_to_long(v);
                    CPGC.idel_time = (int) Z_LVAL_P(v);
                }
                if (cp_zend_hash_find(Z_ARRVAL_P(config), ZEND_STRS("ping_time"), (void **) &v) == SUCCESS)
                {
                    convert_to_long(v);
                    CPGC.ping_time = (int) Z_LVAL_P(v);
                }
            }
        }
        CP_HASHTABLE_FOREACH_END();
        cp_zval_ptr_dtor(&group_conf);
    }
}

static void cpFind_restart_worker(int pid, sigset_t *block_alarm, int worker_exit_code)
{
    int i, j, new_pid;
    for (j = 0; j < CPGS->group_num; j++)
    {
        cpGroup *G = &CPGS->G[j];
        for (i = G->worker_num; i >= 0; i--)
        {
            if (pid != G->workers[i].pid || G->workers_status[i] == CP_WORKER_DEL)
            {//对比pid||回收的不拉起
                continue;
            }
            else
            {
                cpLog("worker exit!worker index %d,worker id %d,exit code %d\n", i, pid, WEXITSTATUS(worker_exit_code));
                cpShareMemory *sm_obj = &(G->workers[i].sm_obj);
                sm_obj->mem = NULL;
                pid = 0;

                char fifo_name[CP_FIFO_NAME_LEN] = {0};
                sprintf(fifo_name, "%s_%d_1", CP_FIFO_NAME_PRE, CP_WORKER_ID(j, i)); //worker 2 client
                int pipe_fd_write = cpCreateFifo(fifo_name);
                cpWorkerInfo worker_event = {0};
                worker_event.type = CP_SIGEVENT_DIE;
                int ret = write(pipe_fd_write, &worker_event, sizeof (worker_event));
                if (ret == -1)
                {
                    cpLog("master write error Error: %s [%d]", strerror(errno), errno);
                }
                close(pipe_fd_write);

                new_pid = cpFork_one_worker(i, j);
                if (new_pid < 0)
                {
                    cpLog("Fork worker process failed. Error: %s [%d]", strerror(errno), errno);
                    sigprocmask(SIG_UNBLOCK, block_alarm, NULL);
                }
                else
                {
                    G->workers[i].pid = new_pid;
                }
            }
        }
    }

}

int cpWorker_manager_loop()
{
    int pid;
    int worker_exit_code;

    //reload config
    cpSignalSet(SIGUSR1, cpManagerReload, 1, 0);
    //close worker
    cpSignalSet(SIGALRM, cpManagerRecycle, 1, 0);
    //add one worker
    cpSignalSet(SIGRTMIN, cpManagerAdd, 1, 0);

    alarm(2);
    sigset_t block_alarm;
    sigemptyset(&block_alarm);
    sigaddset(&block_alarm, SIGALRM);
    sigaddset(&block_alarm, SIGRTMIN);
    sigaddset(&block_alarm, SIGUSR1);

    while (CPGS->running == 1)
    {
        if (CPGS->group_num == 0)
        {//for default max,min 
            sleep(1);
            continue;
        }
        pid = wait(&worker_exit_code);
        sigprocmask(SIG_BLOCK, &block_alarm, NULL);
        if (CPGS->running == 1 && pid > 0)
        {

            if (pid == CPGS->ping_workers->pid)
            {
                cpLog("ping worker exit");
                int ping_pid = cpFork_ping_worker();
                if (ping_pid < 0)
                {
                    cpLog("Fork ping  process fail");
                }
                else
                {
                    CPGS->ping_workers->pid = ping_pid;
                }
            }
            cpFind_restart_worker(pid, &block_alarm, worker_exit_code);
        }
        sigprocmask(SIG_UNBLOCK, &block_alarm, NULL);
    }
    return SUCCESS;
}

CPINLINE int cpCreate_worker_mem(int worker_id, int group_id)
{
    cpShareMemory *sm_obj = &(CPGS->G[group_id].workers[worker_id].sm_obj);
    sprintf(sm_obj->mmap_name, "%s_%d", CP_MMAP_NAME_PRE, CP_WORKER_ID(group_id, worker_id));
    sm_obj->size = CPGS->max_buffer_len;
    if (cp_create_mmap_file(sm_obj) < 0)
    {
        return FAILURE;
    }
    return SUCCESS;
}

CPINLINE int cpWorker_attach_mem(int worker_id, int group_id)
{
    cpShareMemory *sm_obj = &(CPGS->G[group_id].workers[worker_id].sm_obj);
    if (!cp_mmap_calloc_with_file(sm_obj))
    {
        return FAILURE;
    }
    return SUCCESS;
}

//fix the gone away

void cpWorker_do_ping()
{
    zval * stmt_value = NULL;
    zval method, **args[1], *sql = NULL;
    if (CPWG.working == 1)
    {
        alarm(CPGC.ping_time);
        return;
    }
    CP_ZVAL_STRING(&method, "query", 0);
    CP_MAKE_STD_ZVAL(sql);
    CP_ZVAL_STRING(sql, "select 1", 0);
    args[0] = &sql;
    if (pdo_object != NULL)
    {
        cp_call_user_function_ex(NULL, &pdo_object, &method, &stmt_value, 1, args, 0, NULL TSRMLS_CC);
        if (stmt_value)
            cp_zval_ptr_dtor(&stmt_value);
    }
#if PHP_MAJOR_VERSION==7
    zval_ptr_dtor(&method);
    zval_ptr_dtor(sql);
#else
    efree(sql);
#endif
    if (EG(exception))
    {
        cpLog("the connection is broken,we will del it and reconnect at next request,%p", pdo_object);
        CP_DEL_OBJ(pdo_object);
        EG(exception) = NULL;
    }
    alarm(CPGC.ping_time);
}

