/* Copyright (C) 2012 Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#define MYSQL_SERVER 1

#include <sql_class.h>
#include <set_var.h>
#include <mysql_version.h>
#include <mysql/plugin.h>
#include <my_sys.h>
#include <my_global.h>
#include <violite.h>
#include <sql_priv.h>
#include <sql_class.h>
#include <my_pthread.h>
#include <scheduler.h>
#include <sql_connect.h>
#include <sql_audit.h>
#include <debug_sync.h>
#include "threadpool.h"
#include <global_threads.h>
#include <probes_mysql.h>


/* Threadpool parameters */
// XZY
//uint threadpool_min_threads; //win32
uint threadpool_idle_timeout;
uint threadpool_size;
uint threadpool_stall_limit;
uint threadpool_max_threads;
uint threadpool_oversubscribe;
uint threadpool_high_prio_tickets;
ulong threadpool_high_prio_mode;
/* Stats */
TP_STATISTICS tp_stats;


extern "C" pthread_key(struct st_my_thread_var*, THR_KEY_mysys);
extern bool do_command(THD*);

/*
  Worker threads contexts, and THD contexts.
  =========================================
  
  Both worker threads and connections have their sets of thread local variables 
  At the moment it is mysys_var (this has specific data for dbug, my_error and 
  similar goodies), and PSI per-client structure.

  Whenever query is executed following needs to be done:

  1. Save worker thread context.
  2. Change TLS variables to connection specific ones using thread_attach(THD*).
     This function does some additional work , e.g setting up 
     thread_stack/thread_ends_here pointers.
  3. Process query
  4. Restore worker thread context.

  Connection login and termination follows similar schema w.r.t saving and 
  restoring contexts. 

  For both worker thread, and for the connection, mysys variables are created 
  using my_thread_init() and freed with my_thread_end().

*/
struct Worker_thread_context
{
  PSI_thread *psi_thread;
  st_my_thread_var* mysys_var;

  void save()
  {
#ifdef HAVE_PSI_THREAD_INTERFACE
    psi_thread= PSI_THREAD_CALL(get_thread)();
#endif
    mysys_var= (st_my_thread_var *)pthread_getspecific(THR_KEY_mysys);
  }

  void restore()
  {
#ifdef HAVE_PSI_THREAD_INTERFACE
    PSI_THREAD_CALL(set_thread)(psi_thread);
#endif
    pthread_setspecific(THR_KEY_mysys,mysys_var);
    pthread_setspecific(THR_THD, 0);
    pthread_setspecific(THR_MALLOC, 0);
  }
};


/*
  Attach/associate the connection with the OS thread,
*/
static bool thread_attach(THD* thd)
{
  pthread_setspecific(THR_KEY_mysys,thd->mysys_var);
  thd->thread_stack=(char*)&thd;
  thd->store_globals();
#ifdef HAVE_PSI_THREAD_INTERFACE
  //XZY
  PSI_THREAD_CALL(set_thread)(thd->scheduler.m_psi);
#endif
  mysql_socket_set_thread_owner(thd->net.vio->mysql_socket);
  return 0;
}

#ifdef HAVE_PSI_STATEMENT_INTERFACE
extern PSI_statement_info stmt_info_new_packet;
#endif

void threadpool_net_before_header_psi_noop(struct st_net * /* net */,
                                           void * /* user_data */,
                                           size_t /* count */)
{ }

void threadpool_net_after_header_psi(struct st_net *net, void *user_data,
                                     size_t /* count */, my_bool rc)
{
  THD *thd;
  thd= static_cast<THD*> (user_data);
  DBUG_ASSERT(thd != NULL);

  if (thd->m_server_idle)
  {
    /*
      The server just got data for a network packet header,
      from the network layer.
      The IDLE event is now complete, since we now have a message to process.
      We need to:
      - start a new STATEMENT event
      - start a new STAGE event, within this statement,
      - start recording SOCKET WAITS events, within this stage.
      The proper order is critical to get events numbered correctly,
      and nested in the proper parent.
    */
    MYSQL_END_IDLE_WAIT(thd->m_idle_psi);

    if (! rc)
    {
      thd->m_statement_psi= MYSQL_START_STATEMENT(&thd->m_statement_state,
                                                  stmt_info_new_packet.m_key,
                                                  thd->db, thd->db_length,
                                                  thd->charset());

      THD_STAGE_INFO(thd, stage_init);
    }

    /*
      TODO: consider recording a SOCKET event for the bytes just read,
      by also passing count here.
    */
    MYSQL_SOCKET_SET_STATE(net->vio->mysql_socket, PSI_SOCKET_STATE_ACTIVE);

    thd->m_server_idle = false;
  }
}

void threadpool_init_net_server_extension(THD *thd)
{
#ifdef HAVE_PSI_INTERFACE
  /* Start with a clean state for connection events. */
  thd->m_idle_psi= NULL;
  thd->m_statement_psi= NULL;
  thd->m_server_idle= false;
  /* Hook up the NET_SERVER callback in the net layer. */
  thd->m_net_server_extension.m_user_data= thd;
  thd->m_net_server_extension.m_before_header= threadpool_net_before_header_psi_noop;
  thd->m_net_server_extension.m_after_header= threadpool_net_after_header_psi;
  /* Activate this private extension for the mysqld server. */
  thd->net.extension= & thd->m_net_server_extension;
#else
  thd->net.extension= NULL;
#endif
}

int threadpool_add_connection(THD *thd)
{
  int retval=1;
  Worker_thread_context worker_context;
  worker_context.save();

  /*
    Create a new connection context: mysys_thread_var and PSI thread
    Store them in THD.
  */

  pthread_setspecific(THR_KEY_mysys, 0);
  my_thread_init();
  thd->mysys_var= (st_my_thread_var *)pthread_getspecific(THR_KEY_mysys);
  if (!thd->mysys_var)
  {
    /* Out of memory? */
    worker_context.restore();
    return 1;
  }

  /* Create new PSI thread for use with the THD. */
#ifdef HAVE_PSI_THREAD_INTERFACE
  //XZY
  thd->scheduler.m_psi=
    PSI_THREAD_CALL(new_thread)(key_thread_one_connection, thd, thd->thread_id);
#endif


  /* Login. */
  thread_attach(thd);
  ulonglong now= my_micro_time();
  thd->prior_thr_create_utime= now;
  thd->start_utime= now;
  thd->thr_create_utime= now;

  if (!setup_connection_thread_globals(thd))
  {
    bool rc= login_connection(thd);
    MYSQL_AUDIT_NOTIFY_CONNECTION_CONNECT(thd);
    if (!rc)
    {
      prepare_new_connection_state(thd);
      
      /* 
        Check if THD is ok, as prepare_new_connection_state()
        can fail, for example if init command failed.
      */
      if (thd_is_connection_alive(thd))
      {
        retval= 0;
        thd->net.reading_or_writing= 1;
        //XZY
        //thd->skip_wait_timeout= true;
        MYSQL_SOCKET_SET_STATE(thd->net.vio->mysql_socket, PSI_SOCKET_STATE_IDLE);
        thd->m_server_idle= true;
        threadpool_init_net_server_extension(thd);
      }

      MYSQL_CONNECTION_START(thd->thread_id, &thd->security_ctx->priv_user[0],
                             (char *) thd->security_ctx->host_or_ip);
    }
  }
  worker_context.restore();
  return retval;
}


void threadpool_remove_connection(THD *thd)
{

  Worker_thread_context worker_context;
  worker_context.save();

  thread_attach(thd);
  thd->net.reading_or_writing= 0;

  end_connection(thd);
  close_connection(thd, 0);

  thd->release_resources();
  //XZY
  dec_connection_count();

  remove_global_thread(thd);
  delete thd;

  /*
    Free resources associated with this connection: 
    mysys thread_var and PSI thread.
  */
  my_thread_end();

  mysql_cond_broadcast(&COND_thread_count);

  worker_context.restore();
}

/**
 Process a single client request or a single batch.
*/
int threadpool_process_request(THD *thd)
{
  int retval= 0;
  Worker_thread_context  worker_context;
  worker_context.save();

  thread_attach(thd);

  if (thd->killed == THD::KILL_CONNECTION)
  {
    /* 
      killed flag was set by timeout handler 
      or KILL command. Return error.
    */
    retval= 1;
    goto end;
  }


  /*
    In the loop below, the flow is essentially the copy of thead-per-connections
    logic, see do_handle_one_connection() in sql_connect.c

    The goal is to execute a single query, thus the loop is normally executed 
    only once. However for SSL connections, it can be executed multiple times 
    (SSL can preread and cache incoming data, and vio->has_data() checks if it 
    was the case).
  */
  for(;;)
  {
    Vio *vio;
    thd->net.reading_or_writing= 0;
    mysql_audit_release(thd);

    if ((retval= do_command(thd)) != 0)
      goto end;

    if (!thd_is_connection_alive(thd))
    {
      retval= 1;
      goto end;
    }

    vio= thd->net.vio;
    if (!vio->has_data(vio))
    { 
      /* More info on this debug sync is in sql_parse.cc*/
      DEBUG_SYNC(thd, "before_do_command_net_read");
      thd->net.reading_or_writing= 1;
      goto end;
    }
    if (!thd->m_server_idle) {
      MYSQL_SOCKET_SET_STATE(thd->net.vio->mysql_socket, PSI_SOCKET_STATE_IDLE);
      MYSQL_START_IDLE_WAIT(thd->m_idle_psi, &thd->m_idle_state);
      thd->m_server_idle= true;
    }
  }

end:
  if (!retval && !thd->m_server_idle) {
    MYSQL_SOCKET_SET_STATE(thd->net.vio->mysql_socket, PSI_SOCKET_STATE_IDLE);
    MYSQL_START_IDLE_WAIT(thd->m_idle_psi, &thd->m_idle_state);
    thd->m_server_idle= true;
  }

  worker_context.restore();
  return retval;
}


static scheduler_functions tp_scheduler_functions=
{
  0,                                  // max_threads
  //XZY
  tp_init,                            // init
  NULL,                               // init_new_connection_thread
  tp_add_connection,                  // add_connection
  tp_wait_begin,                      // thd_wait_begin
  tp_wait_end,                        // thd_wait_end
  tp_post_kill_notification,          // post_kill_notification
  NULL,                               // end_thread
  tp_end                              // end
};

//XZY
/*
extern "C" {
extern void scheduler_init();
}

void pool_of_threads_scheduler(struct scheduler_functions *func)
{
  *func = tp_scheduler_functions;
  func->max_threads= threadpool_max_threads;
  //XZY
  scheduler_init();

}

*/

// plugin

static void fix_threadpool_size(THD*, struct st_mysql_sys_var *, void*, const void*)
{
  tp_set_threadpool_size(threadpool_size);
}


static void fix_threadpool_stall_limit(THD*, struct st_mysql_sys_var *, void*, const void*)
{
  tp_set_threadpool_stall_limit(threadpool_stall_limit);
}

static MYSQL_SYSVAR_UINT(idle_timeout, threadpool_idle_timeout,
  PLUGIN_VAR_RQCMDARG,
  "Timeout in seconds for an idle thread in the thread pool."
  "Worker thread will be shut down after timeout",
  NULL, NULL, 60, 1, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(oversubscribe, threadpool_oversubscribe,
  PLUGIN_VAR_RQCMDARG,
  "How many additional active worker threads in a group are allowed.",
  NULL, NULL, 3, 1, 1000, 1);

static MYSQL_SYSVAR_UINT(size, threadpool_size,
 PLUGIN_VAR_RQCMDARG,
 "Number of thread groups in the pool. "
 "This parameter is roughly equivalent to maximum number of concurrently "
 "executing threads (threads in a waiting state do not count as executing).",
 NULL, fix_threadpool_size, (uint)16/*my_getncpus()*/, 1, MAX_THREAD_GROUPS, 1);

static MYSQL_SYSVAR_UINT(stall_limit, threadpool_stall_limit, 
 PLUGIN_VAR_RQCMDARG,
 "Maximum query execution time in milliseconds,"
 "before an executing non-yielding thread is considered stalled."
 "If a worker thread is stalled, additional worker thread "
 "may be created to handle remaining clients.",
 NULL, fix_threadpool_stall_limit, 500, 10, UINT_MAX, 1);

static MYSQL_SYSVAR_UINT(high_prio_tickets, threadpool_high_prio_tickets, 
  PLUGIN_VAR_RQCMDARG,
  "Number of tickets to enter the high priority event queue for each "
  "transaction.",
  NULL, NULL , UINT_MAX, 0, UINT_MAX, 1);

const char *threadpool_high_prio_mode_names[]= {"transactions", "statements",
                                                 "none", NullS};
TYPELIB threadpool_high_prio_mode_typelib=
{
   array_elements(threadpool_high_prio_mode_names) - 1, "threadpool_high_prio_mode",
   threadpool_high_prio_mode_names, NULL
};

static MYSQL_SYSVAR_ENUM(high_prio_mode, threadpool_high_prio_mode,
  PLUGIN_VAR_RQCMDARG,
  "High priority queue mode: one of 'transactions', 'statements' or 'none'. "
  "In the 'transactions' mode the thread pool uses both high- and low-priority "
  "queues depending on whether an event is generated by an already started "
  "transaction and whether it has any high priority tickets (see "
  "thread_pool_high_prio_tickets). In the 'statements' mode all events (i.e. "
  "individual statements) always go to the high priority queue, regardless of "
  "the current transaction state and high priority tickets. "
  "'none' is the opposite of 'statements', i.e. disables the high priority queue "
  "completely.",
  NULL, NULL, TP_HIGH_PRIO_MODE_TRANSACTIONS, &threadpool_high_prio_mode_typelib);

static MYSQL_SYSVAR_UINT(max_threads, threadpool_max_threads, 
  PLUGIN_VAR_RQCMDARG,
  "Maximum allowed number of worker threads in the thread pool", 
  NULL, NULL, MAX_CONNECTIONS, 1, MAX_CONNECTIONS, 1);


static int threadpool_plugin_init(void *p)
{
  DBUG_ENTER("threadpool_plugin_init");
  my_thread_scheduler_set(&tp_scheduler_functions);
  //pool_of_threads_scheduler(&tp_scheduler_functions);
  DBUG_RETURN(0);
}


static int threadpool_plugin_deinit(void *p)
{
  DBUG_ENTER("threadpool_plugin_deinit");
  my_thread_scheduler_reset();
  DBUG_RETURN(0);
}

static struct st_mysql_sys_var* system_variables[]= {
  MYSQL_SYSVAR(idle_timeout),
  MYSQL_SYSVAR(size),
  MYSQL_SYSVAR(stall_limit),
  MYSQL_SYSVAR(max_threads),
  MYSQL_SYSVAR(oversubscribe),
  MYSQL_SYSVAR(high_prio_tickets),
  MYSQL_SYSVAR(high_prio_mode),
  NULL
};

struct st_mysql_daemon threadpool_plugin=
{ MYSQL_DAEMON_INTERFACE_VERSION  };

mysql_declare_plugin(threadpool)
{
  MYSQL_DAEMON_PLUGIN,
  &threadpool_plugin,
  "threadpool",
  "Xie Zhenye",
  "thread pool plugin extracted from percona server",
  PLUGIN_LICENSE_GPL,
  threadpool_plugin_init, /* Plugin Init */
  threadpool_plugin_deinit, /* Plugin Deinit */
  0x0100 /* 1.0 */,
  NULL,                       /* status variables                */
  system_variables,           /* system variables                */
  NULL,                       /* config options                  */
  0,                          /* flags                           */
}
mysql_declare_plugin_end;


