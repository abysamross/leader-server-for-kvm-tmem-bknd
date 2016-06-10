#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/init.h>

#include <linux/net.h>
#include <net/sock.h>
#include <linux/tcp.h>
#include <linux/in.h>
#include <asm/uaccess.h>
#include <linux/socket.h>
#include <linux/slab.h>
#include <linux/string.h>

#include "leader_tcp.h"
//#define PORT 2325

//struct socket *conn_socket = NULL;

u32 create_address(u8 *ip)
{
        u32 addr = 0;
        int i;

        for(i=0; i<4; i++)
        {
                //pr_info("%d octet: %u\n", i, ip[i]);
                addr += ip[i];
                if(i==3)
                        break;
                addr <<= 8;
        }
        return addr;
}

u32 create_addr_from_str(char *str)
{
        u32 addr = 0;
        int i;
        u32 j;

        for(i = 0; i < 4; i++)
        {
                j = 0;
                kstrtouint(strsep(&str,"."), 10, &j);
                //pr_info("%d octet: %u\n", i, j);
                addr += j;

                if(i == 3)
                        break;

                addr <<= 8;
        }

        return addr;
}

int leader_client_send(struct socket *sock, const char *buf, const size_t length,\
                unsigned long flags)
{
        struct msghdr msg;
        //struct iovec iov;
        struct kvec vec;
        int len, written = 0, left = length;
        mm_segment_t oldmm;

        msg.msg_name    = 0;
        msg.msg_namelen = 0;
        /*
        msg.msg_iov     = &iov;
        msg.msg_iovlen  = 1;
        */
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
        msg.msg_flags   = flags;

        oldmm = get_fs(); set_fs(KERNEL_DS);
repeat_send:
        /*
        msg.msg_iov->iov_len  = left;
        msg.msg_iov->iov_base = (char *)buf + written; 
        */
        vec.iov_len = left;
        vec.iov_base = (char *)buf + written;

        //len = sock_sendmsg(sock, &msg, left);
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
      //pr_info("msg.msg_iter.kvec->iov_len: %zu\n",msg.msg_iter.kvec->iov_len);
        return written ? written:len;
}

int leader_client_receive(struct socket *sock, char *str,\
                        unsigned long flags)
{
        //mm_segment_t oldmm;
        struct msghdr msg;
        //struct iovec iov;
        struct kvec vec;
        int len;
        int max_size = 50;

        msg.msg_name    = 0;
        msg.msg_namelen = 0;
        /*
        msg.msg_iov     = &iov;
        msg.msg_iovlen  = 1;
        */
        msg.msg_control = NULL;
        msg.msg_controllen = 0;
        msg.msg_flags   = flags;
        /*
        msg.msg_iov->iov_base   = str;
        msg.msg_ioc->iov_len    = max_size; 
        */
        vec.iov_len = max_size;
        vec.iov_base = str;

        //oldmm = get_fs(); set_fs(KERNEL_DS);
read_again:
        //len = sock_recvmsg(sock, &msg, max_size, 0); 
        len = kernel_recvmsg(sock, &msg, &vec, max_size, max_size, flags);

        if(len == -EAGAIN || len == -ERESTARTSYS)
        {
                pr_info(" *** mtp | error while reading: %d | "
                        "tcp_client_receive *** \n", len);

                goto read_again;
        }


        pr_info(" *** mtp | the server says: %s | tcp_client_receive *** \n",str);
        //set_fs(oldmm);
        return len;
}

/*
int tcp_client_fwd_filter(struct *bloom_filter)
{
        send(bloom_filter);
        receive(response);
}

int tcp_client_fwd_page(struct *page)
{
       send(page); 
       receive(response);
}
*/
//int leader_client_fwd_filter(struct socket *conn_socket, int id, char *ip,
//int port /*,struct *bloom_filter)*/)
int leader_client_fwd_filter(struct remote_server *dest_rs,\
                struct remote_server *src_rs)
{
        int lid;
        struct socket *conn_socket;
        int id;
        char *ip;
        int port;
        int len = 49;
        char in_msg[len+1];
        char out_msg[len+1];
        int ret;
        
        struct sockaddr_in *rs_addr;
        char *tmp = NULL;
        int addr_len;

        DECLARE_WAIT_QUEUE_HEAD(bflt_wait);

        lid = src_rs->rs_id;

        rs_addr = kmalloc(sizeof(struct sockaddr_in), GFP_KERNEL);

        conn_socket = dest_rs->lcc_socket;
        id = dest_rs->rs_id;
        ip = dest_rs->rs_ip;
        port = dest_rs->rs_port;
        
        ret = 
        conn_socket->ops->getname(conn_socket, (struct sockaddr *)rs_addr,\
                                                                &addr_len, 2);
        if(ret < 0)
               pr_info(" *** mtp | getname error: %d in leader client[%d] "
                       "to rs[%d] | leader_client_fwd_filter  \n", ret, lid, id);
        else
                tmp = inet_ntoa(&(rs_addr->sin_addr));

resend:
        pr_info("leader client[%d] to rs[%d] sending BFLT to %s:%d\n",
                        lid, id, ip, port);
        if( ret >= 0)
        {
                pr_info("leader client[%d] to rs[%d] details: \n"
                        "rs_ip: %s:%d\n", lid, id, tmp, 
                        ntohs(rs_addr->sin_port));
        }
        if(!tmp)
                kfree(tmp);

        memset(out_msg, 0, len+1);
        snprintf(out_msg, sizeof(out_msg), "RECV:BFLT:%s:%d",\
                        src_rs->rs_ip, src_rs->rs_port);

        ret = leader_client_send(conn_socket, out_msg, strlen(out_msg),\
                        MSG_DONTWAIT);

        pr_info("leader client[%d] to rs[%d] succefully sent: %d bytes\n",
                        lid, id, ret);

        wait_event_timeout(bflt_wait,\
                        !skb_queue_empty(&conn_socket->sk->sk_receive_queue),\
                                                                        5*HZ);
        if(!skb_queue_empty(&conn_socket->sk->sk_receive_queue))
        {
                pr_info("leader client[%d] to rs[%d] receiving message from: "
                        "%s:%d\n", lid, id, ip, port);

                memset(in_msg, 0, len+1);
                ret = leader_client_receive(conn_socket, in_msg, MSG_DONTWAIT);

                if(ret > 0)
                {
                        if(memcmp(in_msg, "BFLT:RECVD", 10) == 0)
                        {
                                pr_info("leader client[%d] to rs[%d] FWD:BFLT "
                                        "success\n", lid, id);
                                goto success; 
                        }
                        else
                        {
                                pr_info("leader client[%d] to rs[%d] re-sending "
                                        "FWD:BFLT\n", lid, id);
                                goto resend;
                        }
                }
        }
        else
        {
                pr_info("leader client[%d] to rs[%d] FWD:BFLT failed\n", lid, id);
                goto fail;
        }

success:
        return 0;
fail:
        return -1;
}

//int leader_client_connect(struct socket *conn_socket, int id, char *dip, int port)
int leader_client_connect(struct remote_server *rs)
{
        struct socket *conn_socket;
        struct sockaddr_in saddr;
        int port;
        int id;
        char *ip; 
        //char *ip2;
        //unsigned char destip[5] = {10,129,26,26,'\0'};
        //u32 destip;
        int ret = -1;
        /*
        struct sockaddr_in daddr;
        struct socket *data_socket = NULL;
        */
        conn_socket = rs->lcc_socket;
        ip = kmalloc(16*(sizeof(char)),GFP_KERNEL);
        //ip2 = kmalloc(16*(sizeof(char)),GFP_KERNEL);
        strcpy(ip, rs->rs_ip);
        //strcpy(ip2, dip);
        port = rs->rs_port; 
        id = rs->rs_id;
        pr_info(" *** mtp | leader client[%d] connecting to remote server: %d | "
                "leader_client_connect *** \n", id, id);
        pr_info("remote server: %d destination ip: %s:%d\n", id, ip, port);

        /*
        char *response = kmalloc(4096, GFP_KERNEL);
        char *reply = kmalloc(4096, GFP_KERNEL);
        */

        //DECLARE_WAITQUEUE(recv_wait, current);
        //DECLARE_WAIT_QUEUE_HEAD(reg_wait);
        
        /*
        ret = sock_create(PF_INET, SOCK_STREAM, IPPROTO_TCP, &conn_socket);
        if(ret < 0)
        {
                pr_info(" *** mtp | Error: %d while creating first socket. | "
                        "setup_connection *** \n", ret);
                goto err;
        }
        */

        /*simply testing*/
        //pr_info("create_address: %u, create_addr_from_str: %u\n",
        //        create_address(destip), create_addr_from_str(ip2));
        //kfree(ip2);
        /*simply testing*/

        memset(&saddr, 0, sizeof(saddr));
        saddr.sin_family = AF_INET;
        saddr.sin_port = htons(port);
        saddr.sin_addr.s_addr = htonl(create_addr_from_str(ip));
        kfree(ip);

        ret = conn_socket->ops->connect(conn_socket, (struct sockaddr *)&saddr\
                        , sizeof(saddr), O_RDWR);
        if(ret && (ret != -EINPROGRESS))
        {
                pr_info(" *** mtp | leader client[%d] error: %d while "
                        "connecting to rs[%d] | leader_client_connect *** \n",
                        id, ret, id);
                goto fail;
        }
/* The portion below this can be inserted into the main ktb code as per
         * need.
         */
        //tcp_client_fwd_filter();
        return 0;
fail:
        sock_release(conn_socket);
        return -1;
}

/*
int network_client_init(void)
{
        pr_info(" *** mtp | leader client init | network_client_init *** \n");
        leader_client_connect();
        return 0;
}
*/

void leader_client_exit(struct socket *conn_socket)
{
        int len = 49;
        char response[len+1];
        char reply[len+1];

        //DECLARE_WAITQUEUE(exit_wait, current);
        DECLARE_WAIT_QUEUE_HEAD(exit_wait);

        memset(&reply, 0, len+1);
        strcat(reply, "ADIOS"); 
        //tcp_client_send(conn_socket, reply);
        leader_client_send(conn_socket, reply, strlen(reply), MSG_DONTWAIT);

        //while(1)
        //{
                /*
                tcp_client_receive(conn_socket, response);
                add_wait_queue(&conn_socket->sk->sk_wq->wait, &exit_wait)
                */
         wait_event_timeout(exit_wait,\
                         !skb_queue_empty(&conn_socket->sk->sk_receive_queue),\
                                                                        5*HZ);
        if(!skb_queue_empty(&conn_socket->sk->sk_receive_queue))
        {
                memset(&response, 0, len+1);
                leader_client_receive(conn_socket, response, MSG_DONTWAIT);
                //remove_wait_queue(&conn_socket->sk->sk_wq->wait, &exit_wait);
        }

        //}

        if(conn_socket != NULL)
        {
                sock_release(conn_socket);
        }
        pr_info(" *** mtp | leader client exiting | network_client_exit *** \n");
}
