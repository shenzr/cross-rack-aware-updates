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

/*
 * This function processes the update request received from the baseline client
 */
void baseline_md_process_req(UPDT_REQ_DATA* req, char* sender_ip){

	int local_chunk_id;
	int global_chunk_id; 
	int node_id;
	int j;
	int stripe_id;

	// read the data from req
	local_chunk_id=req->local_chunk_id;
	stripe_id=local_chunk_id/data_chunks;

	// init a meta_info
	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

	metadata->data_chunk_id=local_chunk_id;
	metadata->stripe_id=stripe_id;
	metadata->port_num=UPDT_PORT;

	// locate each parity node and the storage index of the corresponding parity chunk 
	for(j=0; j<num_chunks_in_stripe-data_chunks; j++){

		global_chunk_id=stripe_id*num_chunks_in_stripe+data_chunks+j;
		metadata->updt_prty_nd_id[j]=global_chunk_map[global_chunk_id];
		metadata->updt_prty_store_index[j]=locate_store_index(metadata->updt_prty_nd_id[j], global_chunk_id);

	}

	// locate the storage node that keeps the data to be updated, and get its storage index
	global_chunk_id=stripe_id*num_chunks_in_stripe+local_chunk_id%data_chunks;
	node_id=global_chunk_map[global_chunk_id];

	metadata->chunk_store_index=locate_store_index(node_id, global_chunk_id);
	memcpy(metadata->next_ip, node_ip_set[node_id], ip_len);

	// send the metadata back to the client
	send_req(NULL, sender_ip, metadata->port_num, metadata, METADATA_INFO);

	free(metadata);

}


int main(int argc, char** argv){

	// it first reads the mapping infomation between chunks and nodes from a mapping file named "chunk_map"
	read_chunk_map("chunk_map");

	// get the stored order of a chunk. A store order can indicate the storage addresses of chunks 
	get_chunk_store_order();

	// listen the request from clients 
	int connfd;
	int recv_len;
	int read_size;
	char* sender_ip;
	int server_socket=init_server_socket(UPDT_PORT);

	char* recv_buff=(char*)malloc(sizeof(UPDT_REQ_DATA));
	UPDT_REQ_DATA* req=(UPDT_REQ_DATA*)malloc(sizeof(UPDT_REQ_DATA));

	// init the sender info
	struct sockaddr_in sender_addr;
	socklen_t length=sizeof(sender_addr);

	if(listen(server_socket, 20) == -1){
		printf("Failed to listen.\n");
		exit(1);
	}

	while(1){

		connfd=accept(server_socket, (struct sockaddr*)&sender_addr, &length);
		if(connfd<0){

			perror("connection fails\n");
			exit(1);

		}

		sender_ip=inet_ntoa(sender_addr.sin_addr); 
		recv_len=0;
		read_size=0;

		// receive the data from the socket and initialize a request structure
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




