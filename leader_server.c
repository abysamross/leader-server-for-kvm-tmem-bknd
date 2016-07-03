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
#include <linux/rwsem.h>
#include "leader_tcp.h"
#define DEFAULT_PORT 2325
#define MODULE_NAME "tmem_tcp_server"
#define MAX_CONNS 16

MODULE_LICENSE("GPL");
MODULE_AUTHOR("Aby Sam Ross");

static int tcp_listener_stopped = 0;
static int tcp_acceptor_stopped = 0;

static DECLARE_RWSEM(rs_rwmutex);
//static DEFINE_RWLOCK(rs_rwspinlock);

LIST_HEAD(rs_head);

struct tcp_conn_handler_data
{
        struct sockaddr_in *address;
        struct socket *accept_socket;
        int thread_id;
        char *ip;
        int port;
        char *in_buf;
        //char *out_buf;
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

int tcp_server_send(struct socket *sock, const char *buf, const size_t length,\
                    unsigned long flags)
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
        
        if((len == -ERESTARTSYS) || (!(flags & MSG_DONTWAIT)&&(len == -EAGAIN)))
                goto repeat_send;

        if(len > 0)
        {
                written += len;
                left -= len;
                if(left)
                        goto repeat_send;
        }
        
        set_fs(oldmm);
        pr_info(" *** mtp | send: %d bytes | tcp_server_send\n", written);
        return written?written:len;
}

//int tcp_server_receive(struct socket *sock, int id,struct sockaddr_in *address,
//                        char *buf,int size, unsigned long flags)
int tcp_server_receive(struct socket *sock, void *rcv_buf,int size,\
                unsigned long flags, int huge)
{
        struct msghdr msg;
        struct kvec vec;
        int len, totread = 0, left = size, count = size;
        char *buf = NULL;
        //unsigned long *bitmap = NULL;
        
        if(sock==NULL)
                return -1;

        buf = (char *)rcv_buf;

        msg.msg_name = 0;
        msg.msg_namelen = 0;
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
        msg.msg_flags = flags;

read_again:

        vec.iov_len = left;
        vec.iov_base = (char *)(buf + totread);

        /*
        if(!skb_queue_empty(&sock->sk->sk_receive_queue))
                pr_info("recv queue empty ? %s \n",
                skb_queue_empty(&sock->sk->sk_receive_queue)?"yes":"no");
        */
        len = kernel_recvmsg(sock, &msg, &vec, left, left, flags);

        if(len == -EAGAIN || len == -ERESTARTSYS)
                goto read_again;

        //pr_info("read: %d\n", len);
        if(huge)
        {
                /* 
                 * you should timeout somehow if you cannot get the entire bloom
                 * filter rather than simply looping around.
                 */
                /* 
                 * you should timeout somehow if you cannot get the entire bloom
                 * filter rather than simply looping around.
                 */
                /*
                 * count and comparison of buf with "FAIL"|"ADIOS" are ugly ways
                 * of ensuring that a huge receive, a blft(32MB) or page(4KB),
                 * doesn't loop forever.
                 * comparison with "FAIL" is to safeguard against the other end
                 * not being able to send entire size.
                 * comparison with "ADIOS" is to safeguard against the other end
                 * quitting in between; this most probably won't happen as the
                 * other end will take care not to quit until and unless it
                 * tries to send the size bytes.
                 * count is to safeguard against an ungraceful exit of the other
                 * end.
                 */
                if(count < 0)
                        goto recv_out;

                count--;

                if(len > 0)
                {
                        //pr_info("len: %d\n", len);
                       //((len == 5) && (memcmp(buf+totread, "ADIOS", 4) == 0)))
                        if((len == 4) && (memcmp(buf+totread, "FAIL", 4) == 0))
                                        goto recv_out;
                        totread += len;
                        //pr_info("total read: %d\n", totread);
                        left -= len;
                        //pr_info("left: %d\n", left);
                        if(left)
                                goto read_again;
                }
        }
        //len = msg.msg_iter.kvec->iov_len;
recv_out:
        pr_info(" *** mtp | return from receive after reading total: %d bytes, "
                "last read: %d bytes | tcp_server_receive \n", totread, len);
        return totread?totread:len;
}

struct remote_server* look_up_rs(char *ip, int port)
{
        struct remote_server *rs_tmp;

        down_read(&rs_rwmutex);
        //read_lock(&rs_rwspinlock);
        if(!(list_empty(&rs_head)))
        {
                list_for_each_entry(rs_tmp, &(rs_head), rs_list)
                {
                        //up_read(&rs_rwmutex);
                        //read_unlock(&rs_rwspinlock);

                        if(strcmp(rs_tmp->rs_ip, ip) == 0)
                        {
                                pr_info(" *** mtp | found remote server "
                                        "info:\n | ip-> %s | port-> %d "
                                        "| look_up_rs ***\n", 
                                        rs_tmp->rs_ip, rs_tmp->rs_port);

                                //read_unlock(&rs_rwspinlock);
                                up_read(&rs_rwmutex);
                                return rs_tmp;
                        }

                        //read_lock(&rs_rwspinlock);
                        //down_read(&rs_rwmutex);
                }
        }
        //else
        //read_unlock(&rs_rwspinlock);
        up_read(&rs_rwmutex);

        return NULL;
}

void inform_others(struct tcp_conn_handler_data *conn, struct remote_server *rs)
{
        struct remote_server *rs_tmp;

        down_read(&rs_rwmutex);
        //read_lock(&rs_rwspinlock);
        if(!(list_empty(&rs_head)))
        {
            list_for_each_entry(rs_tmp, &(rs_head), rs_list)
            {
                    char *ip;
                    int p;
                    /* 
                     * I think I should make a local copy here of the rs_tmp, so
                     * that even if that guy quits and goes, I will have a
                     * referece to him.
                     */

                    //up_read(&rs_rwmutex);
                    //read_unlock(&rs_rwspinlock);

                    ip =
                    inet_ntoa(&(rs_tmp->rs_addr->sin_addr));

                    p = ntohs(rs_tmp->rs_addr->sin_port);

                    pr_info("*** mtp | leader server[%d] remote server info:\n"
                            "id-> %d | ip-> %s | port-> %d | address-> %s:%d |\n"
                            "inform_others *** \n",
                            conn->thread_id, rs_tmp->rs_id, rs_tmp->rs_ip, 
                            rs_tmp->rs_port, ip, p);

                    /*
                     * I should use rs->rs_id, instead of conn->thread_id.
                     * Bcoz when inform_others() is called from within
                     * register_rs() on detecting a duplicate entry in the RS
                     * list, conn->thread_id will have the new thread_id
                     * allocated to the new connection. But rs->rs_id will have
                     * the correct old thread_id which we want.
                     */
                    //if(rs_tmp->rs_id != conn->thread_id)
                    
                    if(rs_tmp->rs_id != rs->rs_id)
                    {
                        leader_client_inform_others(rs_tmp, rs);
                    }

                    kfree(ip);

                    //read_lock(&rs_rwspinlock);
                    //down_read(&rs_rwmutex);
            }
        }
        //read_unlock(&rs_rwspinlock);
        up_read(&rs_rwmutex);
}

//struct remote_server *register_rs(struct socket *socket, char* pkt,
//                int id, struct sockaddr_in *address)
struct remote_server *register_rs(struct socket *socket,\
                                  struct tcp_conn_handler_data *conn) 
{
        int port;
        int ret;
        char *tmp;
        char *ip;
        struct remote_server *rs = NULL;

        tmp = strsep(&conn->in_buf, ":");
        //ip = strsep(&pkt,":");
        
        if(strcmp(tmp, "REGRS") != 0)
        {
                pr_info(" *** mtp | REGRS not in packet | register_rs ***\n");
                return NULL;
        }

        ip = conn->ip;
        kstrtoint(conn->in_buf, 10, &port);
        
        rs = look_up_rs(ip, port);

        if(rs != NULL)
        {
                pr_info(" *** mtp | found an existing RS with ip: %s, id: %d. "
                        "Deleting it and informing others | register_rs *** \n",
                        rs->rs_ip, rs->rs_id);

                inform_others(conn, rs);

                if(tcp_conn_handler->thread[rs->rs_id] != NULL)
                {

                        if(!tcp_conn_handler->tcp_conn_handler_stopped[rs->rs_id])
                        {
                                ret = 
                                kthread_stop(tcp_conn_handler->thread[rs->rs_id]);

                                if(!ret)
                                        pr_info(" *** mtp | tcp server "
                                                "connection handler "
                                                "thread: %d stopped | "
                                                "register_rs "
                                                "*** \n", rs->rs_id);
                        }
               }
        }

        rs = kmalloc(sizeof(struct remote_server), GFP_KERNEL);

        if(!rs)
                return NULL;

        memset(rs, 0, sizeof(struct remote_server));

        rs->lcc_socket = socket; 
        //mutex_init(&rs->lcc_sock_mutex);
        rs->rs_port = port;
        rs->rs_ip = conn->ip;
        //kstrtoint(conn->in_buf, 10, &(rs->rs_port));
        //rs->rs_ip = conn->ip;

        /*
        rs->rs_ip = inet_ntoa(&(address->sin_addr));
        rs->rs_ip = kmalloc(16 * sizeof(char), GFP_KERNEL);
        strcpy(rs->rs_ip, ip);
        kstrtoint(pkt, 10, &(rs->rs_port));
        */

        rs->rs_id = conn->thread_id;
        rs->rs_addr = conn->address;
        rs->rs_bitmap = NULL;
        rs->rs_bmap_size = 0;
        //rs->rs_status = RS_ALIVE;
        
        pr_info(" *** mtp | registered remote server#: %d with ip: %s:%d | "
                "register_rs ***\n",
                rs->rs_id, rs->rs_ip, rs->rs_port);

        down_write(&rs_rwmutex);
        //write_lock(&rs_rwspinlock);
        list_add_tail(&(rs->rs_list), &(rs_head));
        //write_unlock(&rs_rwspinlock);
        up_write(&rs_rwmutex);

        return rs;
}


void fwd_bflt(struct tcp_conn_handler_data *conn, struct remote_server *rs)
{
        struct remote_server *rs_tmp;

        down_read(&rs_rwmutex);
        //read_lock(&rs_rwspinlock);
        if(!(list_empty(&rs_head)))
        {
            list_for_each_entry(rs_tmp, &(rs_head), rs_list)
            {
                    char *ip;
                    int p;

                    /* 
                     * I think I should make a local copy here of the rs_tmp, so
                     * that even if that guy quits and goes, I will have a
                     * referece to him.
                     */
                    //up_read(&rs_rwmutex);
                    //read_unlock(&rs_rwspinlock);

                    ip =
                    inet_ntoa(&(rs_tmp->rs_addr->sin_addr));

                    p = ntohs(rs_tmp->rs_addr->sin_port);

                    pr_info("*** mtp | leader server[%d] remote server info:\n"
                            "id-> %d | ip-> %s | port-> %d | address-> %s:%d |\n"
                            "fwd_bflt *** \n",
                            conn->thread_id, rs_tmp->rs_id, rs_tmp->rs_ip, 
                            rs_tmp->rs_port, ip, p);

                    if(rs_tmp->rs_id != conn->thread_id)
                    {
                        /*
                        if(leader_client_fwd_filter(rs_tmp, rs) < 0)
                                goto recv_bflt_out; 
                         */

                        /* 
                         * the leader_client_fwd_filter function tries to 
                         * forward the bflt a second time, if the first attempt
                         * fails, and if that too fails it just moves onto the
                         * next remote server
                         */

                        /* 
                         * also the retry attempt can be moved to here rather
                         * than from within the leader_client_fwd_filter
                         * function, as it was implemented in normal remote 
                         * server
                         */
                        leader_client_fwd_filter(rs_tmp, rs);
                    }
                    kfree(ip);
                    //read_lock(&rs_rwspinlock);
                    //down_read(&rs_rwmutex);
            }
        }
        //read_unlock(&rs_rwspinlock);
        up_read(&rs_rwmutex);
        
        /*
         * ucomment the 2 lines depending on
         * whether you want to keep the blft
         * bitmap around even after sending it
         * across to all registered RSes.
         * ***********************************
         * rs->rs_bitmap = NULL;
         * vfree(bitmap);
         * ***********************************
         */
}
//int receive_and_fwd_bflt(struct socket *accept_socket,
//                                struct remote_server *rs,int id, char *clip,
//                                                int port, int bmap_bits_size)
int receive_bflt(struct tcp_conn_handler_data *conn, struct remote_server *rs)
{
        int len = 49;
        int ret = 0;
        int bmap_bits_size = 0;
        int bmap_bytes_size = 0;
        int i = 0;
        char out_buf[len+1];
        void *vaddr;
        unsigned long *bitmap;
        /*
        char *tmp;
        int tot_ret = 0;
        char in_buf[len+1];
        struct bloom_filter *bflt;
        int n_pages = 0;
        
        kstrtoint(conn->in_buf+4, 10, &bmap_bits_size);
        tmp = strsep(&(conn->in_buf), ":");
        kstrtoint(tmp, 10, &bmap_bits_size);
        */
        pr_info(" *** mtp | in_buf: %s | receive_bflt \n", conn->in_buf);
        
        kstrtoint(conn->in_buf+10, 10, &bmap_bits_size);

        bmap_bytes_size = BITS_TO_LONGS(bmap_bits_size)*sizeof(unsigned long);

        pr_info(" *** mtp | bmap bits size: %d, bmap bytes size: %d | "
                "receive_bflt \n", bmap_bits_size, bmap_bytes_size);

        bitmap = vmalloc(bmap_bytes_size);

        memset(bitmap, 0, bmap_bytes_size);
        
        for(i = 0; i < bmap_bits_size; i++)
                if(test_bit(i, bitmap))
                        pr_info("%d bit is set\n", i);

        if(!bitmap)
        {
                pr_info(" *** mtp | failed to allocate memory for bflt of "
                        "rs[%d] | receive_bflt *** \n", conn->thread_id);
                goto recv_bflt_out;
        }
        /* announce that you are ready
         * to receive the bflt
         */
        memset(out_buf, 0, len+1);
        strcat(out_buf, "SEND:BFLT");

        pr_info(" *** mtp | leader server[%d] sending response: %s to: %s:%d |"
                "receive_bflt *** \n", 
                conn->thread_id, out_buf, conn->ip, conn->port);
        
        tcp_server_send(conn->accept_socket, out_buf, strlen(out_buf),\
                        MSG_DONTWAIT);
        
        vaddr = (void *)bitmap;

        ret = 
        tcp_server_receive(conn->accept_socket, vaddr, bmap_bytes_size,\
                           MSG_DONTWAIT, 1);

        pr_info(" *** mtp | leader server[%d] received bitmap (size: %d) of "
                "rs[%d] | receive_bflt *** \n",
                conn->thread_id, ret, rs->rs_id);

        if( ret != bmap_bytes_size)
        {
                vfree(bitmap);
                goto recv_bflt_out;
        }

        pr_info(" *** mtp | leader server[%d] testing received bitmap of "
                "rs[%d]\n bitmap[0]: %d, bitmap[10]: %d |\n "
                "receive_bflt *** \n",
                conn->thread_id, rs->rs_id,
                test_bit(0, bitmap), test_bit(10, bitmap));

        for(i = 0; i < bmap_bits_size; i++)
                if(test_bit(i, bitmap))
                        pr_info("%d bit is set\n", i);

        if(rs->rs_bitmap != NULL)
                vfree(rs->rs_bitmap);
        
        //rs->rs_bmap_size = bmap_bytes_size; 
        rs->rs_bmap_size = bmap_bits_size; 
        rs->rs_bitmap = bitmap;

        return 0;

recv_bflt_out:

        return -1;
}

//int create_and_register_rs(struct socket **socket, struct remote_server **rsp,
//                             char *buf, int id, struct sockaddr_in *address)
int create_and_register_rs(struct socket **socket, struct remote_server **rsp,\
                           struct tcp_conn_handler_data *conn)
{
        int err;
        struct remote_server *rs;

        err =  sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP, socket);

        if(err < 0 || *socket == NULL)
        {
              pr_info("*** mtp | error: %d creating "
                      "leader client connection socket |"
                      " connection_handler[%d] ***\n",
                      err, conn->thread_id);

              goto fail;
        }

        //rs = register_rs(*socket, buf, id, address);
        /*
         * before registering make sure somebody with the same credentials
         * doesn't exist. If so, either you remove it and ask others to remove
         * it, Or deny registration,
         * I am doing former inside register_rs.
         */
        rs = register_rs(*socket, conn);

        if(rs == NULL)
        {
                pr_info("*** mtp | error: registering remote server with leader"
                        " server | connection_handler[%d] ***\n",
                        conn->thread_id);

                goto fail;
        }

        err = leader_client_connect(rs); 

        if(err < 0)
        {
              pr_info("*** mtp | error: %d connecting "
                      "leader client to remote server |"
                      " connection_handler[%d] *** \n",
                      err, conn->thread_id);

              down_write(&rs_rwmutex);
              //write_lock(&rs_rwspinlock);
              if(!list_empty(&rs_head))
                      list_del_init(&(rs->rs_list));
               //kfree(rs->rs_ip);
              //write_unlock(&rs_rwspinlock);
              if(rs->rs_bitmap != NULL)
                       vfree(rs->rs_bitmap);
              //rs->rs_status = RS_EXITING;
              //mutex_unlock(&rs->rs_mutex);
              kfree(rs);
              up_write(&rs_rwmutex);
              goto fail;
        }
        *rsp = rs;
        return 0;
fail:
        return -1;
}

int connection_handler(void *data)
{
       int ret; 
       int len = 49;
       char in_buf[len+1];
       char out_buf[len+1];
       struct socket *lc_conn_socket = NULL;
       struct remote_server *rs = NULL;

       struct tcp_conn_handler_data *conn_data = 
       (struct tcp_conn_handler_data *)data;

       //struct sockaddr_in *address = conn_data->address;
       struct socket *accept_socket = conn_data->accept_socket;
       char *ip = conn_data->ip;
       int port = conn_data->port;
       int id = conn_data->thread_id;
       DECLARE_WAITQUEUE(recv_wait, current);
       
       conn_data->in_buf = in_buf;
       //conn_data->out_buf = out_buf;

       allow_signal(SIGKILL|SIGSTOP);
       /*
       while((ret = tcp_server_receive(accept_socket, id, in_buf, len,\
                                       MSG_DONTWAIT)))
       while(tcp_server_receive(accept_socket, id, in_buf, len,\
                                       MSG_DONTWAIT))
       tmp = inet_ntoa(&(address->sin_addr));
       port = ntohs(address->sin_port);
       */

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
                             pr_info(" *** mtp | leader server handle connection"
                                     " thread stopped | connection_handler[%d]"
                                     " *** \n", id);

                              //tcp_conn_handler->thread[id] = NULL;
                              tcp_conn_handler->tcp_conn_handler_stopped[id]= 1;

                              __set_current_state(TASK_RUNNING);
                              remove_wait_queue(&accept_socket->sk->sk_wq->wait,\
                                                &recv_wait);
                              
                              //write_lock(&rs_rwspinlock);

                              down_write(&rs_rwmutex);
                              if(rs != NULL)
                              {

                                      if(!list_empty(&rs_head))
                                              list_del_init(&(rs->rs_list));
                                      
                                      /* putting client exit under write lock is
                                       * fine as I wouldn't want anybody around 
                                       * as I am looking to quit*/

                                      leader_client_exit(rs);

                                      if(rs->rs_bitmap != NULL)
                                              vfree(rs->rs_bitmap);
                                      
                                      kfree(rs);
                              }
                              up_write(&rs_rwmutex);
                                      
                              //up_write(&rs_rwmutex);
                              kfree(tcp_conn_handler->data[id]->address);
                              kfree(tcp_conn_handler->data[id]->ip);
                              kfree(tcp_conn_handler->data[id]);
                              //kfree(tmp);
                              sock_release(tcp_conn_handler->data[id]->\
                                           accept_socket);
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

              pr_info(" *** mtp | leader server[%d] receiving message | " 
                      "connection_handler *** \n", id);
              memset(in_buf, 0, len+1);
              //ret = tcp_server_receive(accept_socket, id, address, in_buf, len,
              //                                        MSG_DONTWAIT);

              ret = tcp_server_receive(accept_socket, in_buf, len,\
                                       MSG_DONTWAIT, 0);

              pr_info(" *** mtp | client-> %s:%d, says: %s | "
                      "connection_handler *** \n", ip, port, in_buf);

              if(ret > 0)
              {
                      if(memcmp(in_buf, "REGRS", 5) == 0)
                      {
                              //struct remote_server *rs;
                              int err;

                              pr_info(" *** mtp | leader server[%d] received: "
                                      "REGRS from: %s:%d | connection_handler "
                                      "*** \n", id, ip, port);
                              /* 
                               * leader server can choose to start a permanent
                               * client connection also at this point with the 
                               * remote server.
                               */
                              err =
                              create_and_register_rs(&lc_conn_socket, &rs,\
                                                     conn_data);

                              if(err < 0)
                                      goto regfail;


                              memset(out_buf, 0, len+1);
                              strcat(out_buf, "RSREGD");
                              goto regresp;
regfail:
                              memset(out_buf, 0, len+1);
                              strcat(out_buf, "FAIL:REGRS");
                              err = -1;
regresp:
                              pr_info(" *** mtp | leader server[%d] sending "
                                      "response: %s to: %s:%d | "
                                      "connection_handler *** \n",
                                      id, out_buf, ip, port);

                              ret = tcp_server_send(accept_socket, out_buf,\
                                                    strlen(out_buf),\
                                                    MSG_DONTWAIT);

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
                      else if(memcmp(in_buf, "FRWD", 4) == 0)
                      {
                              //struct remote_server *rs_tmp;

                              /* in_buf+5 bcoz ':' delimiter is present*/
                              if(memcmp(in_buf+5, "BFLT", 4) == 0)
                              {
                                      pr_info(" *** mtp | leader server[%d] "
                                              "received: FRWD:BFLT from: "
                                              "%s:%d | connection_handler ***\n",
                                              id, ip, port);

                                      conn_data->in_buf = in_buf;

                                      if(receive_bflt(conn_data, rs) < 0)
                                              goto bfltfail;

                                      /* Wait here till you receive entire bloom
                                       * filter. And then for each remote server 
                                       * registered start tranferring the bloom 
                                       * filter.
                                       */

                                     /* 
                                      * the leader server should just receive
                                      * the bloom filter and send done to normal
                                      * remote server and forward the bflt to
                                      * others later??
                                     if(receive_and_fwd_bflt(accept_socket, rs,\
                                                             conn_data,\
                                                             bmap_bits_size) < 0)
                                                                goto bfltfail;
                                      */

                                /* TODO:
                                 * the leader server just sends BFLTFWD w/o
                                 * bothering whether the BFLT was successfully 
                                 * transferred to all remote servers in the 
                                 * previous step
                                 */
                                      memset(out_buf, 0, len+1);
                                      strcat(out_buf, "DONE:BFLT");
                                      goto bfltresp;
bfltfail:
                                      memset(out_buf, 0, len+1);
                                      strcat(out_buf, "FAIL:BFLT");

bfltresp:
                                      pr_info(" *** mtp | leader server[%d] "
                                              "sending response: %s to: "
                                              "%s:%d | connection_handler ***\n",
                                              id, out_buf, ip, port);

                                      tcp_server_send(accept_socket, out_buf,\
                                                      strlen(out_buf),\
                                                      MSG_DONTWAIT);

                                      if(memcmp(out_buf, "DONE:BFLT", 9) == 0)
                                              fwd_bflt(conn_data, rs);
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
                              /* 
                               * the leader server should make sure that when an
                               * rs quits, he informs the others to remove this
                               * rs from their records
                              if(rs)
                              {
                                down_write(&rs_rwmutex);
                                list_del_init(&(rs->rs_list));
                                up_write(&rs_rwmutex);
                              }
                               */
                              memset(out_buf, 0, len+1);
                              strcat(out_buf, "ADIOSAMIGO");

                              pr_info(" *** mtp | leader server[%d] sending "
                                      "response: %s to %s:%d | "
                                      "connection_handler *** \n",
                                      id, out_buf, ip, port);

                              tcp_server_send(accept_socket, out_buf,\
                                              strlen(out_buf), MSG_DONTWAIT);
                              if(rs != NULL)
                              {
                                      conn_data->in_buf = in_buf;
                                      inform_others(conn_data, rs);
                              }
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
       
       /*
        * whent this thread exits shouldn't the rs associated with also be
        * removed. The rs, its bitmap, ip...to free up the memory allocated to
        * them.
        */

out:
       //write_lock(&rs_rwspinlock);
       down_write(&rs_rwmutex);
       if(rs != NULL)
       {
               if(!list_empty(&rs_head))
                       list_del_init(&(rs->rs_list));

               pr_info(" *** mtp | closing leader client[%d] connection | "
                 "connection_handler *** \n", id);

               leader_client_exit(rs);
               
               if(rs->rs_bitmap != NULL)
                       vfree(rs->rs_bitmap);

               kfree(rs);
       }
       //write_unlock(&rs_rwspinlock);
       up_write(&rs_rwmutex);

drop:
       tcp_conn_handler->tcp_conn_handler_stopped[id]= 1;
       kfree(tcp_conn_handler->data[id]->address);
       kfree(tcp_conn_handler->data[id]->ip);
       kfree(tcp_conn_handler->data[id]);
       //kfree(tmp);
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
                char * sip;
                int sport;
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

               pr_info(" *** mtp | accepting incoming connection "
                       "| tcp_server_accept *** \n");

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


               sip = inet_ntoa(&(client->sin_addr));
               sport = ntohs(client->sin_port);

               pr_info(" *** mtp | connection from: %s %d "
                       "| tcp_server_accept *** \n", sip, sport);
                       

               //kfree(tmp);

               /*
               memset(in_buf, 0, len+1);
               pr_info("receive the package\n");
               */
               pr_info(" *** mtp | handle connection | "
                       "tcp_server_accept *** \n");


               /*should I protect this against concurrent access?*/
               for(id = 0; id < MAX_CONNS; id++)
               {
                        //spin_lock(&tcp_server_lock);
                        if(tcp_conn_handler->thread[id] == NULL)
                                break;
                        //spin_unlock(&tcp_server_lock);
               }

               pr_info(" *** mtp | gave free id: %d | "
                       "tcp_server_accept *** \n", id);

               if(id == MAX_CONNS)
                       goto release;

               data = kmalloc(sizeof(struct tcp_conn_handler_data), GFP_KERNEL);
               memset(data, 0, sizeof(struct tcp_conn_handler_data));

               data->address = client;
               data->accept_socket = accept_socket;
               data->thread_id = id;
               data->ip = sip;
               data->port = sport;
               data->in_buf = NULL;
               //data->out_buf = NULL;

               tcp_conn_handler->tcp_conn_handler_stopped[id] = 0;
               tcp_conn_handler->data[id] = data;
               tcp_conn_handler->thread[id] = 
               kthread_run((void *)connection_handler, (void *)data,"leader_connection_handler");

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
        kthread_run((void*)tcp_server_accept, NULL, "leader_server_accept");

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
                                         "leader_server_listen");
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
                                if(pid_alive(tcp_conn_handler->thread[id]))
                                        pr_info(" *** mtp | connection handler "
                                                "thread: %d is not stale and "
                                                "safe to kill | "
                                                "network_server_exit *** \n", id);
                                else
                                        continue;

                        if(!tcp_conn_handler->tcp_conn_handler_stopped[id])
                                {
                                        pr_info(" *** mtp | calling kthread_stop "
                                                "on connection handler thread: %d "
                                                "is not stale and safe to kill | "
                                                "network_server_exit *** \n", id);

                                        ret = 
                                kthread_stop(tcp_conn_handler->thread[id]);

                                        if(!ret)
                                                pr_info(" *** mtp | tcp server "
                                                        "connection handler "
                                                        "thread: %d stopped | "
                                                        "network_server_exit "
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
