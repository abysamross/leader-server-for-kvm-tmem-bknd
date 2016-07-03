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

int leader_client_send(struct socket *sock, void  *snd_buf, const size_t length,\
                       unsigned long flags, int huge)
{
        int len, written = 0, left = length, count = length;
        struct msghdr msg;
        //struct iovec iov;
        struct kvec vec;
        char *buf;
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

        buf = (char *)snd_buf;

        oldmm = get_fs(); set_fs(KERNEL_DS);

repeat_send:
        /*
        msg.msg_iov->iov_len  = left;
        msg.msg_iov->iov_base = (char *)buf + written; 
        */
        vec.iov_len = left;
        vec.iov_base = (char *)(buf + written);

        //len = sock_sendmsg(sock, &msg, left);
        len = kernel_sendmsg(sock, &msg, &vec, left, left);

        if((len == -ERESTARTSYS) || (!(flags & MSG_DONTWAIT)&&(len == -EAGAIN)))
                goto repeat_send;

        //pr_info("written: %d\n", len);

        if(huge)
        {
                if(count < 0)
                        goto send_out;

                count--;

                if(len > 0)
                {
                        //pr_info("written: %d\n", len);
                        written += len;
                        //pr_info("total written: %d\n", written);
                        left -= len;
                        //pr_info("left: %d\n", left);
                        if(left)
                                goto repeat_send;
                }
        }

send_out:

        set_fs(oldmm);
        pr_info(" *** mtp | return from send after writing total: %d bytes, "
                "last write: %d bytes | leader_client_send \n", written, len);
        //pr_info("msg.msg_iter.kvec->iov_len: %zu\n",msg.msg_iter.kvec->iov_len);
        return written ? written:len;
}

int leader_client_receive(struct socket *sock, char *str, unsigned long flags)
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


        pr_info(" *** mtp | the server says: %s | leader_client_receive *** \n",str);
        //set_fs(oldmm);
        return len;
}

void leader_client_inform_others(struct remote_server *dest_rs,
                                 struct remote_server *src_rs)
{
        int len = 49;
        int id;
        int lid;
        int port;
        int ret;
        char out_msg[len+1]; 
        char *ip;
        struct socket *conn_socket;

        conn_socket = dest_rs->lcc_socket;
        id = dest_rs->rs_id;
        ip = dest_rs->rs_ip;
        port = dest_rs->rs_port;
        lid = src_rs->rs_id;

inform_retry:

        memset(out_msg, 0, len+1);
        snprintf(out_msg, sizeof(out_msg), "QUIT:%s:%d",\
                 src_rs->rs_ip, src_rs->rs_port);

        pr_info(" *** mtp | leader client[%d] to rs[%d] sending %s to "
                "%s:%d | leader_client_inform_others ***\n",
                lid, id, out_msg, ip, port);

        ret = leader_client_send(conn_socket, out_msg, strlen(out_msg),\
                                 MSG_DONTWAIT, 0);

        pr_info(" *** mtp | leader client[%d] to rs[%d] succefully sent: %d "
                "bytes | leader_client_inform_others ***\n",
                lid, id, ret);

        if(ret != strlen(out_msg))
                goto inform_retry;
}

//int leader_client_fwd_filter(struct socket *conn_socket, int id, char *ip,
//int port /*,struct *bloom_filter)*/)
int leader_client_fwd_filter(struct remote_server *dest_rs, 
                             struct remote_server *src_rs)
{
        int lid;
        int id;
        int port;
        int len = 49;
        int ret;
        int attempts = 0;
        int addr_len;
        unsigned long jleft;
        char in_msg[len+1];
        char out_msg[len+1];
        char *ip;
        char *tmp = NULL;
        struct socket *conn_socket;
        struct sockaddr_in *rs_addr;

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
                       "to rs[%d] | leader_client_fwd_filter ***\n",
                       ret, lid, id);
        else
               tmp = inet_ntoa(&(rs_addr->sin_addr));

fwd_bflt_resend:

        pr_info(" *** mtp | leader client[%d] to rs[%d] sending BFLT to "
                "%s:%d | leader_client_fwd_filter ***\n",
                lid, id, ip, port);

        if( ret >= 0)
        {
                pr_info(" *** mtp | leader client[%d] to rs[%d] details: \n"
                        "rs_ip: %s:%d | leader_client_fwd_filter ***\n", 
                        lid, id, tmp, ntohs(rs_addr->sin_port));
        }

        if(!tmp)
                kfree(tmp);

        memset(out_msg, 0, len+1);

        snprintf(out_msg, sizeof(out_msg), "RECV:BFLT:%s:%d:%d",\
                 src_rs->rs_ip, src_rs->rs_port, src_rs->rs_bmap_size);

        ret = leader_client_send(conn_socket, out_msg, strlen(out_msg),\
                                 MSG_DONTWAIT, 0);
        
        pr_info(" *** mtp | leader client[%d] to rs[%d] succefully sent: %d "
                "bytes | leader_client_fwd_filter ***\n",
                lid, id, ret);

fwd_bflt_wait:

        jleft = 
        wait_event_timeout(bflt_wait,\
                           (skb_queue_empty(&conn_socket->sk->sk_receive_queue) == 0),\
                           10*HZ);


        if(!skb_queue_empty(&conn_socket->sk->sk_receive_queue))
        {
                pr_info(" *** mtp | wait_event_timeout returned: %lu, secs left"
                        ": %lu | leader_client_fwd_filter *** \n",
                        jleft, jleft/HZ);

                pr_info(" *** mtp | leader client[%d] to rs[%d] receiving "
                        "message from: %s:%d | leader_client_fwd_filter ***\n",
                        lid, id, ip, port);

                memset(in_msg, 0, len+1);

                ret = leader_client_receive(conn_socket, in_msg, MSG_DONTWAIT);

                pr_info(" *** mtp | leader client[%d] to rs[%d] received "
                        "message: %s from: %s:%d | "
                        "leader_client_fwd_filter ***\n",
                        lid, id, in_msg, ip, port);

                if(ret > 0)
                {
                        if(memcmp(in_msg, "SEND", 4) == 0)
                        {
                                if(memcmp(in_msg+5, "BFLT", 4) == 0)
                                {
                                        int bmap_byte_size = 
                      BITS_TO_LONGS(src_rs->rs_bmap_size)*sizeof(unsigned long);
                                        
                                        ret = 
                                        leader_client_send(conn_socket,\
                                                           src_rs->rs_bitmap,\
                                                           bmap_byte_size,\
                                                           MSG_DONTWAIT, 1);

                                        pr_info(" *** mtp | leader client[%d] "
                                                "to rs[%d] sent: %d bytes | "
                                                "leader_client_fwd_filter *** \n",
                                                lid, id, ret);

                                        if(ret != bmap_byte_size)
                                        {
                                                msleep(5000);
                                                memset(out_msg, 0, len+1);        
                                                strcat(out_msg, "FAIL");

                                                ret = 
                                                leader_client_send(conn_socket,\
                                                                out_msg,\
                                                                strlen(out_msg),\
                                                                MSG_DONTWAIT, 0);
                                        }
                                        goto fwd_bflt_wait;
                                }
                        }
                        else if(memcmp(in_msg, "FAIL", 4) == 0)
                        {
                                if(memcmp(in_msg+5, "BFLT", 4) == 0)
                                {
                                        if(attempts == 1)
                                                goto fwd_bflt_fail;

                                        /*
                                         * this retry can be moved to place of
                                         * invocation of
                                         * leader_client_fwd_filter function,
                                         * rather than handling it here, as it
                                         * was done in normal remote server client
                                         */
                                        attempts++;
                                        pr_info(" *** mtp | leader client[%d] to "
                                                "rs[%d] re-sending FWD:BFLT | " 
                                                "leader_client_fwd_filter *** \n",
                                                lid, id);

                                        goto fwd_bflt_resend;
                                }
                        }
                        else if(memcmp(in_msg, "DONE", 4) == 0)            
                        {                                                   
                                if(memcmp(in_msg+5, "BFLT", 4) == 0)
                                {
                                        pr_info(" *** mtp | leader client[%d] to"
                                                " rs[%d] FWD:BFLT success | "
                                                "leader_client_fwd_filter ***\n",
                                                lid, id);
                                }
                        }
                        else
                        {
                                goto fwd_bflt_fail;
                        }
                }
        }
        else
        {
                pr_info(" *** mtp | leader client[%d] to rs[%d] FWD:BFLT "
                        "failed | leader_client_fwd_filter ***\n", lid, id);
                goto fwd_bflt_fail;
        }

        return 0;

fwd_bflt_fail:

        pr_info(" *** mtp | leader client[%d] to rs[%d] FWD:BFLT "
                "failed | leader_client_fwd_filter ***\n", lid, id);
        return -1;
}

//int leader_client_connect(struct socket *conn_socket, int id, char *dip,
//int port)
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

        pr_info(" *** mtp | remote server: %d destination ip: %s:%d | "
                "leader_client_connect ***\n", id, ip, port);

        memset(&saddr, 0, sizeof(saddr));
        saddr.sin_family = AF_INET;
        saddr.sin_port = htons(port);
        saddr.sin_addr.s_addr = htonl(create_addr_from_str(ip));
        kfree(ip);

        ret = conn_socket->ops->connect(conn_socket, (struct sockaddr *)&saddr,\
                                        sizeof(saddr), O_RDWR);
        
        pr_info(" *** mtp | connection attempt return value: %d | "
                "leader_client_connect \n", ret);

        if(ret && (ret != -EINPROGRESS))
        {
                pr_info(" *** mtp | leader client[%d] error: %d while "
                        "connecting to rs[%d] | leader_client_connect *** \n",
                        id, ret, id);
                goto fail;
        }

        return 0;

fail:

        return -1;
}

//void leader_client_exit(struct socket *conn_socket)
void leader_client_exit(struct remote_server *rs)
{
        int len = 49;
        unsigned long jleft;
        char response[len+1];
        char reply[len+1];
        struct socket *conn_socket = rs->lcc_socket;

        //DECLARE_WAITQUEUE(exit_wait, current);
        DECLARE_WAIT_QUEUE_HEAD(exit_wait);

        memset(&reply, 0, len+1);

        strcat(reply, "ADIOS"); 
        //tcp_client_send(conn_socket, reply);

        leader_client_send(conn_socket, reply, strlen(reply), MSG_DONTWAIT, 0);

        jleft = 
        wait_event_timeout(exit_wait,\
                           (skb_queue_empty(&conn_socket->sk->sk_receive_queue)\
                            == 0), 5*HZ);

        if(!skb_queue_empty(&conn_socket->sk->sk_receive_queue))
        {
                pr_info(" *** mtp | wait_event_timeout returned: %lu, secs left"
                        ": %lu | leader_client_exit *** \n", jleft, jleft/HZ);

                memset(&response, 0, len+1);
                
                leader_client_receive(conn_socket, response, MSG_DONTWAIT);
                //remove_wait_queue(&conn_socket->sk->sk_wq->wait, &exit_wait);
        }

        if(conn_socket != NULL)
        {
                sock_release(conn_socket);
        }

        pr_info(" *** mtp | leader client exiting | leader_client_exit *** \n");
}
