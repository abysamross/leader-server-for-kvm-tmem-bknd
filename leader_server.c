#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/init.h>
#include <linux/slab.h>
#include <linux/kthread.h>

#include <linux/errno.h>
#include <linux/types.h>

#include <linux/netdevice.h>
#include <linux/ip.h>
#include <linux/in.h>

#include <linux/unistd.h>
#include <linux/wait.h>

#include <net/sock.h>
#include <net/tcp.h>
#include <net/inet_connection_sock.h>
#include <net/request_sock.h>

#include <linux/list.h>
#include <linux/string.h>
#include "leader_tcp.h"
#define DEFAULT_PORT 2325
#define MODULE_NAME "tmem_tcp_server"
#define MAX_CONNS 16

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Aby Sam Ross");

static int tcp_listener_stopped = 0;
static int tcp_acceptor_stopped = 0;

DEFINE_SPINLOCK(tcp_server_lock);
static DECLARE_RWSEM(rs_rwmutex);

LIST_HEAD(rs_head);

struct tcp_conn_handler_data
{
        struct sockaddr_in *address;
        struct socket *accept_socket;
        int thread_id;
};

struct tcp_conn_handler
{
        struct tcp_conn_handler_data *data[MAX_CONNS];
        struct task_struct *thread[MAX_CONNS];
        int tcp_conn_handler_stopped[MAX_CONNS]; 
};

struct tcp_conn_handler *tcp_conn_handler;

struct tcp_server_service
{
      int running;  
      struct socket *listen_socket;
      struct task_struct *thread;
      struct task_struct *accept_thread;
};

struct tcp_server_service *tcp_server;

struct remote_server *register_rs(struct socket *socket, char* pkt,\
                int id, struct sockaddr_in *address)
{
        struct remote_server *rs = NULL;
        char *tmp;
        //int port;
        //char *ip;

        tmp = strsep(&pkt, ":");
        //ip = strsep(&pkt,":");
        
        if(strcmp(tmp, "REGRS") != 0)
        {
                pr_info("REGRS not in packet\n");
                return NULL;
        }

        rs = kmalloc(sizeof(struct remote_server), GFP_KERNEL);

        if(!rs)
                return NULL;

        kstrtoint(pkt, 10, &(rs->rs_port));
        rs->lcc_socket = socket; 
        rs->rs_ip = inet_ntoa(&(address->sin_addr));
        //rs->rs_ip = kmalloc(16 * sizeof(char), GFP_KERNEL);
        //strcpy(rs->rs_ip, ip);
        //kstrtoint(pkt, 10, &(rs->rs_port));

        rs->rs_id = id;
        rs->rs_addr = address;
        
        pr_info("registered remote server#: %d with ip: %s:%d\n",
                        rs->rs_id, rs->rs_ip, rs->rs_port);

        down_write(&rs_rwmutex);
        list_add_tail(&(rs->rs_list), &(rs_head));
        up_write(&rs_rwmutex);

        return rs;
}

char *inet_ntoa(struct in_addr *in)
{
        char *str_ip = NULL;
        u_int32_t int_ip = 0;
        
        str_ip = kmalloc(16 * sizeof(char), GFP_KERNEL);

        if(!str_ip)
                return NULL;
        else
                memset(str_ip, 0, 16);

        int_ip = in->s_addr;

        sprintf(str_ip, "%d.%d.%d.%d", (int_ip) & 0xFF, (int_ip >> 8) & 0xFF,
                                 (int_ip >> 16) & 0xFF, (int_ip >> 24) & 0xFF);
        
        return str_ip;
}

int tcp_server_send(struct socket *sock, int id, const char *buf,\
                const size_t length, unsigned long flags)
{
        struct msghdr msg;
        struct kvec vec;
        int len, written = 0, left =length;
        mm_segment_t oldmm;

        msg.msg_name    = 0;
        msg.msg_namelen = 0;
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
        msg.msg_flags = flags;
        msg.msg_flags   = 0;

        oldmm = get_fs(); set_fs(KERNEL_DS);

repeat_send:
        vec.iov_len = left;
        vec.iov_base = (char *)buf + written;

        len = kernel_sendmsg(sock, &msg, &vec, left, left);
        
        if((len == -ERESTARTSYS) || (!(flags & MSG_DONTWAIT) &&\
                                (len == -EAGAIN)))
                goto repeat_send;

        if(len > 0)
        {
                written += len;
                left -= len;
                if(left)
                        goto repeat_send;
        }
        
        set_fs(oldmm);
        return written?written:len;
}

int tcp_server_receive(struct socket *sock, int id,struct sockaddr_in *address,\
                        char *buf,int size, unsigned long flags)
{
        struct msghdr msg;
        struct kvec vec;
        int len;
        char *tmp = NULL;
        
        if(sock==NULL)
        {
                pr_info(" *** mtp | leader server[%d] receive socket is NULL| "
                        " tcp_server_receive *** \n", id);
                return -1;
        }

        msg.msg_name = 0;
        msg.msg_namelen = 0;
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
        msg.msg_flags = flags;

        vec.iov_len = size;
        vec.iov_base = buf;

read_again:

        /*
        if(kthread_should_stop())
        {
                pr_info(" *** mtp | tcp server handle connection thread "
                        "stopped | tcp_server_receive *** \n");
                //tcp_conn_handler->thread[id] = NULL;
                tcp_conn_handler->tcp_conn_handler_stopped[id]= 1;
                //sock_release(sock);
                do_exit(0);
        }
        */

        if(!skb_queue_empty(&sock->sk->sk_receive_queue))
                pr_info("recv queue empty ? %s \n",
                skb_queue_empty(&sock->sk->sk_receive_queue)?"yes":"no");

        len = kernel_recvmsg(sock, &msg, &vec, size, size, flags);

        if(len == -EAGAIN || len == -ERESTARTSYS)
                goto read_again;
        
        tmp = inet_ntoa(&(address->sin_addr));

        pr_info("client-> %s:%d, says: %s\n", tmp, ntohs(address->sin_port), buf);

        kfree(tmp);
        //len = msg.msg_iter.kvec->iov_len;
        return len;
}

int connection_handler(void *data)
{
       struct tcp_conn_handler_data *conn_data = 
               (struct tcp_conn_handler_data *)data;

       struct sockaddr_in *address = conn_data->address;
       struct socket *accept_socket = conn_data->accept_socket;
       int id = conn_data->thread_id;

       /* 
        * this is the leader client socket to be used for communication with
        * remove server
        */
       struct socket *lc_conn_socket = NULL;
       char *tmp;
       int port;

       int ret; 
       int len = 49;
       char in_buf[len+1];
       char out_buf[len+1];
       struct remote_server *rs = NULL;

       DECLARE_WAITQUEUE(recv_wait, current);
       allow_signal(SIGKILL|SIGSTOP);
       /*
       while((ret = tcp_server_receive(accept_socket, id, in_buf, len,\
                                       MSG_DONTWAIT)))
       while(tcp_server_receive(accept_socket, id, in_buf, len,\
                                       MSG_DONTWAIT))
       */
       tmp = inet_ntoa(&(address->sin_addr));
       port = ntohs(address->sin_port);

       while(1)
       {
               /*
               if(kthread_should_stop())
               {
                       pr_info(" *** mtp | tcp server acceptor thread "
                               "stopped | tcp_server_accept *** \n");
                       tcp_acceptor_stopped = 1;
                       do_exit(0);
               }
               if(ret == 0)
               */

              add_wait_queue(&accept_socket->sk->sk_wq->wait, &recv_wait);  

              while(skb_queue_empty(&accept_socket->sk->sk_receive_queue))
              {
                      __set_current_state(TASK_INTERRUPTIBLE);
                      schedule_timeout(HZ);

                      if(kthread_should_stop())
                      {
                             pr_info(" *** mtp | leader server handle connection "
                                "thread stopped | connection_handler[%d] *** \n", id);

                              //tcp_conn_handler->thread[id] = NULL;
                              tcp_conn_handler->tcp_conn_handler_stopped[id]= 1;

                              __set_current_state(TASK_RUNNING);
                              remove_wait_queue(&accept_socket->sk->sk_wq->wait,\
                                              &recv_wait);
                              kfree(tcp_conn_handler->data[id]->address);
                              kfree(tcp_conn_handler->data[id]);
                              kfree(tmp);

                              if(lc_conn_socket)
                              {
                                      leader_client_exit(lc_conn_socket);
                              }

                        sock_release(tcp_conn_handler->data[id]->accept_socket);
                              /*
                              tcp_conn_handler->thread[id] = NULL;
                              sock_release(sock);
                              do_exit(0);
                              */
                              return 0;
                      }

                      if(signal_pending(current))
                      {
                              __set_current_state(TASK_RUNNING);
                              remove_wait_queue(&accept_socket->sk->sk_wq->wait,\
                                              &recv_wait);
                              /*
                              kfree(tcp_conn_handler->data[id]->address);
                              kfree(tcp_conn_handler->data[id]);
                          sock_release(tcp_conn_handler->data[id]->accept_socket);
                              */
                              goto out;
                      }
              }
              __set_current_state(TASK_RUNNING);
              remove_wait_queue(&accept_socket->sk->sk_wq->wait, &recv_wait);

              pr_info("leader server[%d] receiving message\n", id);
              memset(in_buf, 0, len+1);
              ret = tcp_server_receive(accept_socket, id, address, in_buf, len,\
                                       MSG_DONTWAIT);
              if(ret > 0)
              {
                      if(memcmp(in_buf, "REGRS", 5) == 0)
                      {
                              //struct remote_server *rs;
                              int err;

                              pr_info("leader server[%d] received: REGRS "
                                      "from: %s:%d\n", id, tmp, port);
                              /* 
                               * leader server can choose to start a permanent
                               * client connection also at this point with the 
                               * remote server.
                               */
                              err = 
                              sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP,\
                                                &lc_conn_socket);
                              
                              if(err < 0 || !lc_conn_socket)
                              {
                                      pr_info("*** mtp | error: %d creating "
                                              "leader client connection socket |"
                                              " connection_handler[%d]\n",
                                             err, id);
                                      goto fail;
                              }

                              rs = register_rs(lc_conn_socket, in_buf, id,\
                                              address);
                              if(!rs)
                              {
                                      pr_info("*** mtp | error: registering "
                                             "remote server with leader server | "
                                             "connection_handler[%d]\n", id);

                                      goto fail;
                              }

                              err = 
                              leader_client_connect(rs); 
                              //leader_client_connect(lc_conn_socket, rs->rs_id,
                              //                rs->rs_ip, rs->rs_port);

                              if(err < 0)
                              {
                                      pr_info("*** mtp | error: %d connecting "
                                              "leader client to remote server |"
                                              " connection_handler[%d]\n",
                                             err, id);

                                      down_write(&rs_rwmutex);
                                      list_del_init(&(rs->rs_list));
                                      up_write(&rs_rwmutex);
                                      kfree(rs->rs_ip);
                                      kfree(rs);
                                      goto fail;
                              }


                              memset(out_buf, 0, len+1);
                              strcat(out_buf, "RSREGD");
                              goto send;
fail:
                              memset(out_buf, 0, len+1);
                              strcat(out_buf, "FAIL");
                              err = -1;
send:
                              pr_info("leader server[%d] sending response: "
                                      "%s to: %s:%d\n", id, out_buf, tmp, port);

                              tcp_server_send(accept_socket, id, out_buf,\
                                              strlen(out_buf), MSG_DONTWAIT);

                              //if(strcmp(out_buf,"FAIL") == 0)
                              if(err < 0)
                              {
                                      pr_info(" *** mtp | closing connection "
                                              "handler: %d | "
                                              "connection_handler[%d] *** \n", 
                                              id, id);
                                      goto drop;
                              }
                      }
                      else if(memcmp(in_buf, "FWD", 3) == 0)
                      {
                              struct remote_server *rs_tmp;

                              /* in_buf+4 bcoz ':' delimiter is present*/
                              if(memcmp(in_buf+4, "BFLT", 4) == 0)
                              {
                                      //tmp = inet_ntoa(&(address->sin_addr));
                                      //port = ntohs(address->sin_port);

                                      pr_info("leader server[%d] received: "
                                              "FWD:BFLT from: %s:%d\n", 
                                              id, tmp, port);
                                      /* for each remote server registered
                                       * start tranferring the bloom filter
                                       */
                                down_read(&rs_rwmutex);
                                if(!(list_empty(&rs_head)))
                                {
                                    list_for_each_entry(rs_tmp, &(rs_head), rs_list)
                                    {
                                            char *ip;
                                            int p;

                                            ip =
                                            inet_ntoa(&(rs_tmp->rs_addr->sin_addr));
                                            p = ntohs(rs_tmp->rs_addr->sin_port);

                                            pr_info("leader server[%d] remote "
                                                    "server info:\n id-> %d | "
                                                    "ip-> %s | port-> %d | "
                                                    "address-> %s:%d\n",
                                                    id, rs_tmp->rs_id, rs_tmp->rs_ip, 
                                                    rs_tmp->rs_port, ip, p);

                                            if(rs_tmp->rs_id != id)
                                            {
                                                //leader_client_fwd_filter(
                                                //rs->lcc_socket, id, rs->rs_ip,
                                                //rs->rs_port);
                                                leader_client_fwd_filter(rs_tmp, id\
                                                        /*,bloom_filter*/);
                                            }
                                            kfree(ip);
                                    }
                                }
                                up_read(&rs_rwmutex);
                                /* TODO:
                                 * the leader server just sends BFLTFWD w/o
                                 * bothering whether the BFLT was successfully 
                                 * transferred to all remote servers in the 
                                 * previous step
                                 */
                                      memset(out_buf, 0, len+1);
                                      strcat(out_buf, "BFLTFWD");

                                      pr_info("leader server[%d] sending "
                                              "response: %s to: %s:%d\n", 
                                              id, out_buf, tmp, port);

                                      tcp_server_send(accept_socket, id, out_buf,\
                                                strlen(out_buf), MSG_DONTWAIT);
                              
                              }
                              else if(memcmp(in_buf+3, "PAGE", 4) == 0)
                              {
                                      /* for the identified remote server 
                                       * forward this page.
                                       */
                              }
                      }
                      else if(memcmp(in_buf, "ADIOS", 5) == 0)
                      {
                              //char *tmp;
                              //int port;

                              //tmp = inet_ntoa(&(address->sin_addr));
                              //port = ntohs(address->sin_port);
                              if(!rs)
                              {
                                down_write(&rs_rwmutex);
                                list_del_init(&(rs->rs_list));
                                up_write(&rs_rwmutex);
                                kfree(rs->rs_ip);
                                kfree(rs);
                              }

                              memset(out_buf, 0, len+1);
                              strcat(out_buf, "ADIOSAMIGO");

                              pr_info("leader server[%d] sending response: %s "
                                      "to %s:%d\n", id, out_buf, tmp, port);
                              tcp_server_send(accept_socket, id, out_buf,\
                                              strlen(out_buf), MSG_DONTWAIT);
                              break;
                      }

              }
       }

       /* 
       tmp = inet_ntoa(&(address->sin_addr));

       pr_info("connection handler: %d of: %s %d exiting normally\n",
                       id, tmp, ntohs(address->sin_port));
       kfree(tmp);
       */

out:
       if(lc_conn_socket)
       {
         pr_info("closing leader client[%d] connection\n", id);
         leader_client_exit(lc_conn_socket);
       }
drop:
       tcp_conn_handler->tcp_conn_handler_stopped[id]= 1;
       kfree(tcp_conn_handler->data[id]->address);
       kfree(tcp_conn_handler->data[id]);
       kfree(tmp);
       sock_release(tcp_conn_handler->data[id]->accept_socket);
       //spin_lock(&tcp_server_lock);
       tcp_conn_handler->thread[id] = NULL;
       //spin_unlock(&tcp_server_lock);
       //return 0;
       do_exit(0);
}

int tcp_server_accept(void)
{
        int accept_err = 0;
        struct socket *socket;
        struct socket *accept_socket = NULL;
        struct inet_connection_sock *isock; 
        int id = 0;
        /*
        int len = 49;
        unsigned char in_buf[len+1];
        unsigned char out_buf[len+1];
        */
        DECLARE_WAITQUEUE(accept_wait, current);
        /*
        spin_lock(&tcp_server_lock);
        tcp_server->running = 1;
        current->flags |= PF_NOFREEZE;
        */
        allow_signal(SIGKILL|SIGSTOP);
        //spin_unlock(&tcp_server_lock);

        socket = tcp_server->listen_socket;
        pr_info(" *** mtp | creating the accept socket | tcp_server_accept "
                "*** \n");
        /*
        accept_socket = 
        (struct socket*)kmalloc(sizeof(struct socket), GFP_KERNEL);
        */

        while(1)
        {
                struct tcp_conn_handler_data *data = NULL;
                struct sockaddr_in *client = NULL;
                char *tmp;
                int addr_len;

                accept_err =  
                        sock_create(socket->sk->sk_family, socket->type,\
                                    socket->sk->sk_protocol, &accept_socket);
                        /*
                        sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP,\
                                        &accept_socket);
                        */

                if(accept_err < 0 || !accept_socket)
                {
                        pr_info(" *** mtp | accept_error: %d while creating "
                                "tcp server accept socket | "
                                "tcp_server_accept *** \n", accept_err);
                        goto err;
                }

                accept_socket->type = socket->type;
                accept_socket->ops  = socket->ops;

                isock = inet_csk(socket->sk);

        //while(1)
        //{
               /*
               struct tcp_conn_handler_data *data = NULL;
               struct sockaddr_in *client = NULL;
               char *tmp;
               int addr_len;
               */
                
               add_wait_queue(&socket->sk->sk_wq->wait, &accept_wait);
               while(reqsk_queue_empty(&isock->icsk_accept_queue))
               {
                       __set_current_state(TASK_INTERRUPTIBLE);
                       //set_current_state(TASK_INTERRUPTIBLE);

                       //change this HZ to about 5 mins in jiffies
                       schedule_timeout(HZ);

                        //pr_info("icsk queue empty ? %s \n",
                //reqsk_queue_empty(&isock->icsk_accept_queue)?"yes":"no");

                        //pr_info("recv queue empty ? %s \n",
                //skb_queue_empty(&socket->sk->sk_receive_queue)?"yes":"no");
                       if(kthread_should_stop())
                       {
                               pr_info(" *** mtp | tcp server acceptor thread "
                                       "stopped | tcp_server_accept *** \n");
                               tcp_acceptor_stopped = 1;
                               __set_current_state(TASK_RUNNING);
                               remove_wait_queue(&socket->sk->sk_wq->wait,\
                                               &accept_wait);
                               sock_release(accept_socket);
                               //do_exit(0);
                               return 0;
                       }

                       if(signal_pending(current))
                       {
                               __set_current_state(TASK_RUNNING);
                               remove_wait_queue(&socket->sk->sk_wq->wait,\
                                               &accept_wait);
                               goto release;
                       }

               } 
               __set_current_state(TASK_RUNNING);
               remove_wait_queue(&socket->sk->sk_wq->wait, &accept_wait);

               pr_info("accept connection\n");

               accept_err = 
                       socket->ops->accept(socket, accept_socket, O_NONBLOCK);

               if(accept_err < 0)
               {
                       pr_info(" *** mtp | accept_error: %d while accepting "
                               "tcp server | tcp_server_accept *** \n",
                               accept_err);
                       goto release;
               }

               client = kmalloc(sizeof(struct sockaddr_in), GFP_KERNEL);   
               memset(client, 0, sizeof(struct sockaddr_in));

               addr_len = sizeof(struct sockaddr_in);

               accept_err = 
               accept_socket->ops->getname(accept_socket,\
                               (struct sockaddr *)client,\
                               &addr_len, 2);

               if(accept_err < 0)
               {
                       pr_info(" *** mtp | accept_error: %d in getname "
                               "tcp server | tcp_server_accept *** \n",
                               accept_err);
                       goto release;
               }


               tmp = inet_ntoa(&(client->sin_addr));

               pr_info("connection from: %s %d \n",
                       tmp, ntohs(client->sin_port));

               kfree(tmp);

               /*
               memset(in_buf, 0, len+1);
               pr_info("receive the package\n");
               */
               pr_info("handle connection\n");

               /*
               while((accept_err = tcp_server_receive(accept_socket, in_buf,\
                                               len, MSG_DONTWAIT)))
               {*/
                       /* not needed here
                       if(kthread_should_stop())
                       {
                               pr_info(" *** mtp | tcp server acceptor thread "
                                       "stopped | tcp_server_accept *** \n");
                               tcp_acceptor_stopped = 1;
                               do_exit(0);
                       }
                       */
                /*
                       if(accept_err == 0)
                               continue;

                       memset(out_buf, 0, len+1);
                       strcat(out_buf, "kernel server: hi");
                       pr_info("sending the package\n");
                       tcp_server_send(accept_socket, out_buf, strlen(out_buf),\
                                       MSG_DONTWAIT);
               }
               */

               /*should I protect this against concurrent access?*/
               for(id = 0; id < MAX_CONNS; id++)
               {
                        //spin_lock(&tcp_server_lock);
                        if(tcp_conn_handler->thread[id] == NULL)
                                break;
                        //spin_unlock(&tcp_server_lock);
               }

               pr_info("gave free id: %d\n", id);

               if(id == MAX_CONNS)
                       goto release;

               data = kmalloc(sizeof(struct tcp_conn_handler_data), GFP_KERNEL);
               memset(data, 0, sizeof(struct tcp_conn_handler_data));

               data->address = client;
               data->accept_socket = accept_socket;
               data->thread_id = id;

               tcp_conn_handler->tcp_conn_handler_stopped[id] = 0;
               tcp_conn_handler->data[id] = data;
               tcp_conn_handler->thread[id] = 
               kthread_run((void *)connection_handler, (void *)data, MODULE_NAME);

               if(kthread_should_stop())
               {
                       pr_info(" *** mtp | tcp server acceptor thread stopped"
                               " | tcp_server_accept *** \n");
                       tcp_acceptor_stopped = 1;
                       /*
                       kfree(tcp_conn_handler->data[id]->address);
                       kfree(tcp_conn_handler->data[id]);
                       sock_release(tcp_conn_handler->data[id]->accept_socket);
                       kfree(accept_socket);
                       do_exit(0);
                       */
                       return 0;
               }
                        
               if(signal_pending(current))
               {
                       /*
                       kfree(tcp_conn_handler->data[id]->address);
                       kfree(tcp_conn_handler->data[id]);
                       goto err;
                       */
                       break;
               }
        //}
        }

        /*
        kfree(tcp_conn_handler->data[id]->address);
        kfree(tcp_conn_handler->data[id]);
        sock_release(tcp_conn_handler->data[id]->accept_socket);
        */
        tcp_acceptor_stopped = 1;
        //return 0;
        do_exit(0);
release: 
       sock_release(accept_socket);
err:
       tcp_acceptor_stopped = 1;
       //return -1;
       do_exit(0);
}

int tcp_server_listen(void)
{
        int server_err;
        struct socket *conn_socket;
        struct sockaddr_in server;

        DECLARE_WAIT_QUEUE_HEAD(wq);

        //spin_lock(&tcp_server_lock);
        //tcp_server->running = 1;
        allow_signal(SIGKILL|SIGTERM);         
        //spin_unlock(&tcp_server_lock);

        server_err = sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP,\
                                &tcp_server->listen_socket);
        if(server_err < 0)
        {
                pr_info(" *** mtp | Error: %d while creating tcp server "
                        "listen socket | tcp_server_listen *** \n", server_err);
                goto err;
        }

        conn_socket = tcp_server->listen_socket;
        tcp_server->listen_socket->sk->sk_reuse = 1;

        server.sin_addr.s_addr = htonl(INADDR_ANY);
        //server.sin_addr.s_addr = INADDR_ANY;
        server.sin_family = AF_INET;
        server.sin_port = htons(DEFAULT_PORT);

        server_err = 
        conn_socket->ops->bind(conn_socket, (struct sockaddr*)&server,\
                        sizeof(server));

        if(server_err < 0)
        {
                pr_info(" *** mtp | Error: %d while binding tcp server "
                        "listen socket | tcp_server_listen *** \n", server_err);
                goto release;
        }

        //while(1)
        //{
        server_err = conn_socket->ops->listen(conn_socket, 16);

        if(server_err < 0)
        {
                pr_info(" *** mtp | Error: %d while listening in tcp "
                        "server listen socket | tcp_server_listen "
                        "*** \n", server_err);
                        goto release;
        }

        tcp_server->accept_thread = 
                kthread_run((void*)tcp_server_accept, NULL, MODULE_NAME);

        while(1)
        {
                wait_event_timeout(wq, 0, 3*HZ);

                if(kthread_should_stop())
                {
                        pr_info(" *** mtp | tcp server listening thread"
                                " stopped | tcp_server_listen *** \n");
                        /*
                        tcp_listener_stopped = 1;
                        sock_release(conn_socket);
                        do_exit(0);
                        */
                        return 0;
                }

                if(signal_pending(current))
                        goto release;
        }
        //}

        sock_release(conn_socket);
        tcp_listener_stopped = 1;
        //return 0;
        do_exit(0);
release:
        sock_release(conn_socket);
err:
        tcp_listener_stopped = 1;
        //return -1;
        do_exit(0);
}

int tcp_server_start(void)
{
        tcp_server->running = 1;
        tcp_server->thread = kthread_run((void *)tcp_server_listen, NULL,\
                                        MODULE_NAME);
        if(tcp_listener_stopped)
                return -1;

        return 0;
}

static int __init network_server_init(void)
{
        pr_info(" *** mtp | initiating network_server | "
                "network_server_init ***\n");
        tcp_server = kmalloc(sizeof(struct tcp_server_service), GFP_KERNEL);
        memset(tcp_server, 0, sizeof(struct tcp_server_service));

        tcp_conn_handler = kmalloc(sizeof(struct tcp_conn_handler), GFP_KERNEL);
        memset(tcp_conn_handler, 0, sizeof(struct tcp_conn_handler));

        //INIT_LIST_HEAD(&(rs_head.rs_list));

        if(tcp_server_start() < 0)
                return -1; 

        if(tcp_listener_stopped)
                return -1;

        return 0;
}

static void __exit network_server_exit(void)
{
        int ret;
        int id;

        if(tcp_server->thread == NULL)
                pr_info(" *** mtp | No kernel thread to kill | "
                        "network_server_exit *** \n");
        else
        {
                for(id = 0; id < MAX_CONNS; id++)
                {
                        if(tcp_conn_handler->thread[id] != NULL)
                        {

                        if(!tcp_conn_handler->tcp_conn_handler_stopped[id])
                                {
                                        ret = 
                                kthread_stop(tcp_conn_handler->thread[id]);

                                        if(!ret)
                                                pr_info(" *** mtp | tcp server "
                                                "connection handler thread: %d "
                                                "stopped | network_server_exit "
                                                "*** \n", id);
                                }
                       }
                }

                if(!tcp_acceptor_stopped)
                {
                        ret = kthread_stop(tcp_server->accept_thread);
                        if(!ret)
                                pr_info(" *** mtp | tcp server acceptor thread"
                                        " stopped | network_server_exit *** \n");
                }

                if(!tcp_listener_stopped)
                {
                        ret = kthread_stop(tcp_server->thread);
                        if(!ret)
                                pr_info(" *** mtp | tcp server listening thread"
                                        " stopped | network_server_exit *** \n");
                }


                if(tcp_server->listen_socket != NULL && !tcp_listener_stopped)
                {
                        sock_release(tcp_server->listen_socket);
                        tcp_server->listen_socket = NULL;
                }

                kfree(tcp_conn_handler);
                kfree(tcp_server);
                tcp_server = NULL;
        }

        pr_info(" *** mtp | network server module unloaded | "
                "network_server_exit *** \n");
}
module_init(network_server_init)
module_exit(network_server_exit)
