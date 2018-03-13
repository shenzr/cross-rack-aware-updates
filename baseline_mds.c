#define _GNU_SOURCE


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>

#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>
#include <arpa/inet.h>

#include "common.h"
#include "config.h"

void baseline_md_process_req(UPDT_REQ_DATA* req, char* sender_ip){

   int local_chunk_id;
   int global_chunk_id; 
   int node_id;
   int j;
   int rack_id;
   int prty_rack_id;
   int stripe_id;
   int chunk_id_in_stripe;

   //read the data from req
   local_chunk_id=req->local_chunk_id;
   stripe_id=local_chunk_id/data_chunks;
   chunk_id_in_stripe=local_chunk_id%data_chunks;

   //init a meta_info
   META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

   metadata->data_chunk_id=local_chunk_id;
   metadata->stripe_id=stripe_id;
   metadata->port_num=UPDT_PORT;

   for(j=0; j<num_chunks_in_stripe-data_chunks; j++){

	 global_chunk_id=stripe_id*num_chunks_in_stripe+data_chunks+j;
	 metadata->updt_prty_nd_id[j]=global_chunk_map[global_chunk_id];
	 metadata->updt_prty_store_index[j]=locate_store_index(metadata->updt_prty_nd_id[j], global_chunk_id);

   	}

   global_chunk_id=stripe_id*num_chunks_in_stripe+local_chunk_id%data_chunks;
   node_id=global_chunk_map[global_chunk_id];
   
   metadata->chunk_store_index=locate_store_index(node_id, global_chunk_id);
   memcpy(metadata->next_ip, node_ip_set[node_id], ip_len);

   send_req(NULL, sender_ip, metadata->port_num, metadata, METADATA_INFO);

   free(metadata);

}


int main(int argc, char** argv){

    read_chunk_map("chunk_map");
	get_chunk_store_order();

	//listen the request from clients 
	int connfd;
	int server_socket=init_server_socket(UPDT_PORT);

	char* sender_ip;
	int recv_len;
	int read_size;

	char* recv_buff=(char*)malloc(sizeof(UPDT_REQ_DATA));
	UPDT_REQ_DATA* req=(UPDT_REQ_DATA*)malloc(sizeof(UPDT_REQ_DATA));

    //init the sender info
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    if(listen(server_socket, 20) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	while(1){

		printf("before accept:\n");
		connfd=accept(server_socket, (struct sockaddr*)&sender_addr, &length);
		printf("connfd=%d\n", connfd);
		
		if(connfd<0){

			perror(connfd);
			exit(1);

			}

		sender_ip=inet_ntoa(sender_addr.sin_addr); 

		recv_len=0;
		read_size=0;
		while(recv_len < sizeof(UPDT_REQ_DATA)){

			read_size=read(connfd, recv_buff+recv_len, sizeof(UPDT_REQ_DATA)-recv_len);
			recv_len += read_size;
			
			}

		memcpy(req, recv_buff, sizeof(UPDT_REQ_DATA));

		baseline_md_process_req(req, sender_ip);

		close(connfd);
		
		}

	free(recv_buff);
	free(req);

    return 0;
}




