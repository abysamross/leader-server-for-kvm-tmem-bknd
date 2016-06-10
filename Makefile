obj-m += leader_tcp.o
leader_tcp-objs += leader_client.o leader_server.o 

all:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules
clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean

