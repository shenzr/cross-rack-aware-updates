#define _GNU_SOURCE

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>

#include "common.h"
#include "config.h"

#define DATA 0
#define ACK  1

double aver_read_time;
double aver_para_send_recv_time;
double aver_send_ack_time;
int updt_count;

int updt_cmlt_count[num_chunks_in_stripe-data_chunks];

/*
 * This function is a thread process to receive a ack 
 */
void* baseline_recv_ack_process(void* ptr){

	RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

	int recv_len;
	int read_size;

	char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

	//receive data
	recv_len=0;
	while(recv_len < sizeof(ACK_DATA)){

		read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(ACK_DATA)-recv_len);
		recv_len+=read_size;

	}

	// copy the receive data and generate an ack
	ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
	memcpy(ack, recv_buff, sizeof(ACK_DATA));

	// check if the ack notifies the completion of a parity update operation 
	if(ack->op_type==PRTY_UPDT_CMPLT)
		updt_cmlt_count[ack->updt_prty_id]++;

	free(ack);
	free(recv_buff);

	return NULL;

}



/*
 * This function receives data by using multiple threads
 */ 
void baseline_para_recv_ack(int stripe_id, int num_recv_chnks, int port_num, int recv_type){

	int i;
	int server_socket;
	int max_connctn;
	int index;
	int* connfd=malloc(sizeof(int)*num_recv_chnks);

	struct sockaddr_in sender_addr;
	socklen_t length=sizeof(sender_addr);

	max_connctn=100;
	index=0;

	pthread_t recv_data_thread[data_chunks];

	// initial socket information
	server_socket=init_server_socket(port_num);

	if(listen(server_socket,max_connctn) == -1){
		printf("Failed to listen.\n");
		exit(1);
	}

	RECV_PROCESS_DATA* rpd=(RECV_PROCESS_DATA *)malloc(sizeof(RECV_PROCESS_DATA)*num_recv_chnks);
	memset(rpd, 0, sizeof(RECV_PROCESS_DATA)*num_recv_chnks);

	while(1){

		connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
		rpd[index].connfd=connfd[index];
		rpd[index].recv_id=index;

		pthread_create(&recv_data_thread[index], NULL, baseline_recv_ack_process, (void *)(rpd+index));

		index++;

		if(index>=num_recv_chnks)
			break;
	}

	// join threads and close connections
	for(i=0; i<num_recv_chnks; i++)
		pthread_join(recv_data_thread[i], NULL);

	for(i=0; i<num_recv_chnks; i++)
		close(connfd[i]);

	free(rpd);
	free(connfd);
	close(server_socket);

}


/*
 * This function calculates the parity delta chunk and sends the parity delta chunk to all the m parity chunks
 */
void baseline_para_send_dt_prty(TRANSMIT_DATA* td, int op_type, char* old_data){

	int j; 
	int its_prty_node_id;
	int its_prty_rack_id;
	int node_id, rack_id;

	node_id=get_local_node_id();
	rack_id=get_rack_id(node_id);

	TRANSMIT_DATA* td_mt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*(num_chunks_in_stripe-data_chunks));
	char* data_delta=(char*)malloc(sizeof(char)*chunk_size);
	char* prty_delta=(char*)malloc(sizeof(char)*chunk_size);

	pthread_t parix_updt_thread[num_chunks_in_stripe-data_chunks];
	memset(parix_updt_thread, 0, sizeof(parix_updt_thread));

	// calculate data delta
	bitwiseXor(data_delta, td->buff, old_data, chunk_size);

	// for each parity node 
	for(j=0; j<num_chunks_in_stripe-data_chunks; j++){

		// init td structure
		td_mt[j].send_size=sizeof(TRANSMIT_DATA);
		td_mt[j].op_type=op_type; 
		td_mt[j].stripe_id=td->stripe_id;
		td_mt[j].data_chunk_id=td->data_chunk_id;
		td_mt[j].num_recv_chks_itn=-1;
		td_mt[j].num_recv_chks_prt=-1;
		td_mt[j].port_num=UPDT_PORT;
		td_mt[j].prty_delta_app_role=-1; 
		td_mt[j].updt_prty_id=j;

		// set the store index of the old parity chunk on each parity node 
		td_mt[j].chunk_store_index=td->updt_prty_store_index[j];

		// calculate parity delta
		encode_data(data_delta, prty_delta, td->data_chunk_id, j);

		// copy the parity delta 
		memcpy(td_mt[j].buff, prty_delta, sizeof(char)*chunk_size);

		its_prty_node_id=td->updt_prty_nd_id[j]; 
		its_prty_rack_id=get_rack_id(its_prty_node_id);

		//printf("Update parity at: ");
		//print_amazon_vm_info(node_ip_set[its_prty_node_id]);

		if((if_gateway_open==1) && (rack_id!=its_prty_rack_id)){
			memcpy(td_mt[j].next_ip, node_ip_set[its_prty_node_id], ip_len);
			memcpy(td_mt[j].sent_ip, gateway_ip, ip_len);
		}

		else
			memcpy(td_mt[j].sent_ip, node_ip_set[its_prty_node_id], ip_len);

		pthread_create(&parix_updt_thread[j], NULL, send_updt_data_process, td_mt+j);

	}

	// join the threads
	for(j=0; j<num_chunks_in_stripe-data_chunks; j++)
		pthread_join(parix_updt_thread[j], NULL);

	free(td_mt);
	free(prty_delta);
	free(data_delta);

}

/*
 * This function is performed by the storage node for processing a update request from the client
 */
void baseline_server_update(TRANSMIT_DATA* td){

	char* old_data=(char*)malloc(chunk_size);

	// read old data   
	read_old_data(old_data, td->chunk_store_index);

	// calculate the parity delta and send it to the corresponding parity nodes
	baseline_para_send_dt_prty(td, PRTY_UPDT, old_data);

	// listen ack
	int sum_cmplt;
	memset(updt_cmlt_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));

	// receive acks by using multi-thread
	baseline_para_recv_ack(td->stripe_id, num_chunks_in_stripe-data_chunks, UPDT_ACK_PORT, ACK);

	// to check if receiving the acks from all the parity nodes 
	sum_cmplt=sum_array(num_chunks_in_stripe-data_chunks, updt_cmlt_count);
	if(sum_cmplt!=(num_chunks_in_stripe-data_chunks)){

		printf("update error! sum_cmplt=%d\n", sum_cmplt);
		exit(1);

	}

	// write the new data to the storage 
	write_new_data(td->buff, td->chunk_store_index);

	// send ack to the client
	send_ack(td->stripe_id, td->data_chunk_id, -1, client_ip, UPDT_ACK_PORT, UPDT_CMLT);

	free(old_data);
}


/*
 * This function is performed by the parity node to update a parity chunk for a given data update request
 */
void baseline_prty_updt(TRANSMIT_DATA* td, char* sender_ip){

	char* old_prty=(char*)malloc(sizeof(char)*chunk_size);
	char* new_prty=(char*)malloc(sizeof(char)*chunk_size);

	// read the old parity
	read_old_data(old_prty, td->chunk_store_index);

	// calculate the new parity based on the old parity and parity delta
	bitwiseXor(new_prty, old_prty, td->buff, chunk_size);

	// write the new parity 
	write_new_data(new_prty, td->chunk_store_index);

	// check if the sender is the gateway
	if((strcmp(sender_ip, gateway_ip)==0) && (if_gateway_open==1))
		memcpy(sender_ip, td->from_ip, ip_len);

	// send an ack back
	send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, sender_ip, UPDT_ACK_PORT, PRTY_UPDT_CMPLT);

	free(old_prty);
	free(new_prty);
}


int main(int argc, char** argv){

	int server_socket;
	int read_size;
	int recv_len;
	int connfd = 0;
	int recv_data_type;

	aver_read_time=0;
	aver_para_send_recv_time=0;
	aver_send_ack_time=0;
	updt_count=0;

	char local_ip[ip_len];
	char* sender_ip;
	int gateway_count;
	int send_size;

	if_gateway_open=GTWY_OPEN;

	// initial encoding coefficinets based on the function from Jerasure
	encoding_matrix=reed_sol_vandermonde_coding_matrix(data_chunks, num_chunks_in_stripe-data_chunks, w);

	// initial socket information
	server_socket=init_server_socket(UPDT_PORT);

	if(listen(server_socket,100) == -1){
		printf("Failed to listen.\n");
		exit(1);
	}

	// init the sender info
	struct sockaddr_in sender_addr;
	socklen_t length=sizeof(sender_addr);

	// init the received info
	TRANSMIT_DATA* td = (TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
	ACK_DATA* ack_dt=(ACK_DATA*)malloc(sizeof(ACK_DATA));
	char* recv_buff = (char*)malloc(sizeof(TRANSMIT_DATA));
	char* recv_head = (char*)malloc(head_size);

	gateway_count=0;

	while(1){

		connfd = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
		if(connfd<0){
			perror("Accpet fails!\n");
			exit(1);
		}

		sender_ip=inet_ntoa(sender_addr.sin_addr);

		recv_len=0;
		send_size=-1;
		recv_len=0;

		// first read a part of data to determine the size of transmitted data
		while(recv_len < head_size){
			read_size=read(connfd, recv_head+recv_len, head_size-recv_len);
			recv_len+=read_size;
			//printf("recv_len=%d, head_size=%lu\n", recv_len, head_size);
		}

		memcpy(&send_size, recv_head, sizeof(int));
		memcpy(recv_buff, recv_head, read_size);

		recv_data_type=-1;

		// read the remaining data from the socket
		while(recv_len < send_size){

			read_size=read(connfd, recv_buff+recv_len, send_size-recv_len);
			recv_len+=read_size;

		}

		// if it contains the updated data
		if(send_size==sizeof(TRANSMIT_DATA)){

			recv_data_type=UPDT_DATA;
			memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

		}

		// else it is an ack info
		else if(send_size==sizeof(ACK_DATA)){

			recv_data_type=ACK_INFO;
			memcpy(ack_dt, recv_buff, sizeof(ACK_DATA));

		}

		else {

			printf("ERR: unrecognized_send_size! send_size=%d\n", send_size);
			exit(1);

		}

		// if the gateway server is started and this server is the gateway server
		// then just forward the data
		if((strcmp(gateway_ip, local_ip)==0) && (if_gateway_open==1)){

			if(recv_data_type==UPDT_DATA)
				gateway_forward_updt_data(td, sender_ip);

			else 
				gateway_forward_ack_info(ack_dt);

			gateway_count++;

			close(connfd);

			continue;

		}

		// if it is a data update request from the client
		if(td->op_type==DATA_UPDT && recv_data_type==UPDT_DATA){


			baseline_server_update(td);
			updt_count++;
		}

		// if it is a parity update request from a data server 
		else if(td->op_type==PRTY_UPDT && recv_data_type==UPDT_DATA)
			baseline_prty_updt(td, sender_ip);


		close(connfd);

	}

	free(td);
	free(recv_head);
	free(recv_buff);
	close(server_socket);
	free(ack_dt);

}


