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

#define num_tlrt_strp 10 //it is to avoid the last write may cause the num_updt_stripes > max_updt_strps, so we have to consider this case

int mark_updt_stripes_tab[(max_updt_strps+num_tlrt_strp)*(data_chunks+1)]; //it records the updated data chunks and their stripes, the data_chunk column stores the data, while the data_chunk-th column store the stripe_id
int cross_rack_updt_traffic;
int num_rcrd_strp;

void* parix_send_cmd_process(void* ptr){

	CMD_DATA pcd=*(CMD_DATA*)ptr;

	printf("Send commit cmd to: ");
	print_amazon_vm_info(pcd.sent_ip);

	send_data(NULL, pcd.sent_ip, pcd.port_num, NULL, (CMD_DATA*)ptr, CMD_INFO);

	return NULL;

}

/*
 * This function performs delta commit in parix 
 */
void parix_commit(int num_rcrd_strp){

	int i,j; 
	int prty_node_id;
	int global_chunk_id;
	int prty_id;
	int sum_ack;

	// initiate the cmd structure to notify the m parity nodes for delta commit 
	CMD_DATA* pcd_prty=(CMD_DATA*)malloc(sizeof(CMD_DATA)*(num_chunks_in_stripe-data_chunks));

	pthread_t send_cmd_thread[num_chunks_in_stripe-data_chunks];
	memset(send_cmd_thread, 0, sizeof(send_cmd_thread));

	// parix performs the delta commit stripe by stripe
	for(i=0; i<num_rcrd_strp; i++){

		memset(commit_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));

		for(prty_id=0; prty_id<num_chunks_in_stripe-data_chunks; prty_id++){

			// init the updated data chunks
			memset(pcd_prty[prty_id].parix_updt_data_id, -1, sizeof(int)*data_chunks);

			// record the updated data chunks in each updated stripe 
			for(j=0; j<data_chunks; j++){

				if(mark_updt_stripes_tab[i*(data_chunks+1)+j+1]>=0)
					pcd_prty[prty_id].parix_updt_data_id[j]=1;

			}

			// notify all the parity chunks to absorb the updates
			pcd_prty[prty_id].send_size=sizeof(CMD_DATA);
			pcd_prty[prty_id].op_type=PARIX_CMMT;
			pcd_prty[prty_id].stripe_id=mark_updt_stripes_tab[i*(data_chunks+1)];
			pcd_prty[prty_id].updt_prty_id=prty_id;
			pcd_prty[prty_id].port_num=UPDT_PORT;
			pcd_prty[prty_id].prty_delta_app_role=PARITY;
			pcd_prty[prty_id].data_chunk_id=-1;

			global_chunk_id=pcd_prty[prty_id].stripe_id*num_chunks_in_stripe+data_chunks+prty_id;
			prty_node_id=global_chunk_map[global_chunk_id];
			pcd_prty[prty_id].chunk_store_index=locate_store_index(prty_node_id, global_chunk_id);

			if(if_gateway_open==1){

				memcpy(pcd_prty[prty_id].next_ip, node_ip_set[prty_node_id], ip_len);
				memcpy(pcd_prty[prty_id].sent_ip, gateway_ip, ip_len);

			}

			else 
				memcpy(pcd_prty[prty_id].sent_ip, node_ip_set[prty_node_id], ip_len);

		}

		// send the commit cmd to m corresponding parity nodes in each stripe 
		for(prty_id=0; prty_id<num_chunks_in_stripe-data_chunks; prty_id++)
			pthread_create(&send_cmd_thread[prty_id], NULL, parix_send_cmd_process, pcd_prty+prty_id);

		// join the threads
		for(prty_id=0; prty_id<num_chunks_in_stripe-data_chunks; prty_id++)
			pthread_join(send_cmd_thread[prty_id], NULL);

		// parallel listen ack
		para_recv_ack(mark_updt_stripes_tab[i*(data_chunks+1)], num_chunks_in_stripe-data_chunks, CMMT_PORT);

		// check if all the m acks are collected
		sum_ack=sum_array(num_chunks_in_stripe-data_chunks, commit_count);

		if(sum_ack!=(num_chunks_in_stripe-data_chunks)){
			printf("Commit Error, sum_ack=%d\n", sum_ack);
			exit(1);
		}

	}

	free(pcd_prty);

}

/*
 * This function processes the update request received from the parix client
 */
void parix_md_process_req(UPDT_REQ_DATA* req, char* sender_ip){

	int local_chunk_id;
	int global_chunk_id; 
	int node_id;
	int j;
	int stripe_id;
	int chunk_id_in_stripe;

	// if the number of stripes that the updated data chunks span is more than max_updt_strps, then invoke commit 
	if(num_rcrd_strp>=max_updt_strps){

		if(num_rcrd_strp>max_updt_strps+num_tlrt_strp){
			printf("ERR: num_rcrd_strp is too large!\n");
			exit(1);
		}

		parix_commit(num_rcrd_strp);
		memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));
		num_rcrd_strp=0;

	}

	// read the logical chunk id from the request
	local_chunk_id=req->local_chunk_id;
	stripe_id=local_chunk_id/data_chunks;
	chunk_id_in_stripe=local_chunk_id%data_chunks;

	// check if the stripe has data chunks updated during the data logging
	// if not, then record the stripe
	for(j=0; j<num_rcrd_strp; j++){
		if(mark_updt_stripes_tab[j*(data_chunks+1)]==stripe_id)
			break;
	}

	if(j>=num_rcrd_strp){
		mark_updt_stripes_tab[j*(data_chunks+1)]=stripe_id;
		num_rcrd_strp++;
	}

	// record the updated data chunks in the k-th stripe
	mark_updt_stripes_tab[j*(data_chunks+1)+chunk_id_in_stripe+1]++;

	// init a meta_info
	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

	metadata->data_chunk_id=local_chunk_id;
	metadata->stripe_id=stripe_id;
	metadata->port_num=UPDT_PORT;

	if(mark_updt_stripes_tab[j*(data_chunks+1)+chunk_id_in_stripe+1]==0)
		metadata->if_first_update=1;

	else 
		metadata->if_first_update=0;

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

	// fill the ip address of the data node
	memcpy(metadata->next_ip, node_ip_set[node_id], ip_len);

	// send the metadata back to the client
	send_req(NULL, client_ip, metadata->port_num, metadata, METADATA_INFO);

	free(metadata);

}


int main(int argc, char** argv){

	// it first reads the mapping infomation between chunks and nodes from a mapping file named "chunk_map"
	read_chunk_map("chunk_map");

	// get the stored order of a chunk. A store order can indicate the storage addresses of chunks 
	get_chunk_store_order();

	// init num_rcrd_strp, which records the number of stripes that the data chunks are updated during data logging
	num_rcrd_strp=0;
	memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));

	// listen the request from clients 
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
		parix_md_process_req(req, sender_ip);

		close(connfd);

	}

	free(recv_buff);
	free(req);

	return 0;

}
