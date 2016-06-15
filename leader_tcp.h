#ifndef _LTCP_H_
#define _LTCP_H_

struct bloom_filter {
	struct kref		kref;
	struct mutex		lock;
	struct list_head	alg_list;
	unsigned int		bitmap_size;
	unsigned long		bitmap[0];
};

struct remote_server
{
        struct socket *lcc_socket;
        /*
         * lcc_socket; the socket using which leader
         * client communicates with remote server.
         */
        int rs_id;
        char *rs_ip;
        int rs_port;
        struct sockaddr_in *rs_addr;
        //unsigned long rs_bitmap[0];
        unsigned long *rs_bitmap;
        int rs_bmap_size;
        struct list_head rs_list;
};

extern u32 create_address(u8 *);
extern u32 create_addr_from_str(char *);
extern char *(inet_ntoa)(struct in_addr *);
extern int leader_client_fwd_filter(struct remote_server *, struct remote_server *);
extern int leader_client_connect(struct remote_server *);
extern void leader_client_exit(struct socket *);
#endif
