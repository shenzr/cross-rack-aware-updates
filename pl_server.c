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


#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>

#include "common.h"
#include "config.h"

#define DATA 0
#define ACK  1

#define LOG_OLD 0
#define LOG_NEW 1

int   pl_log_cnt;

//it is performed at the data chunk by 1) in-place writing the new data; 2) sending the new data to m parity chunks
void pl_update(TRANSMIT_DATA* td){

   int sum_cmplt;
   
   char* old_data=(char*)malloc(sizeof(char)*chunk_size);
   char* data_delta=(char*)malloc(sizeof(char)*chunk_size);
   
   //read old data
   read_old_data(old_data, td->chunk_store_index);

   //encode and send parity delta in parallel to the parity nodes 
   baseline_para_send_dt_prty(td, PRTY_LOG , old_data);

   memset(prty_log_cmplt_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));
   
   para_recv_ack(td->stripe_id, num_chunks_in_stripe-data_chunks, UPDT_ACK_PORT);

   sum_cmplt=sum_array(num_chunks_in_stripe-data_chunks, prty_log_cmplt_count);

   if(sum_cmplt!=(num_chunks_in_stripe-data_chunks)){

	  printf("update error! sum_cmplt=%d\n", sum_cmplt);
	  exit(1);

   	}

   //in-place update the new data 
   printf("td->chunk_store_index=%d\n", td->chunk_store_index);
   write_new_data(td->buff, td->chunk_store_index);

   //send ack to the client node
   send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, mt_svr_ip, UPDT_PORT, LOG_CMLT);
   
}


//it is performed at the parity chunk that log the new data in the log file
void pl_log(TRANSMIT_DATA* td, char* recver_ip, int op_type){

	int i;
	int if_new_log_chnk;

	printf("op_type==PARIX_LGWT\n");

	//specify fd at the bottom of the file
	log_write("pl_log_file", td);

    if_new_log_chnk=update_loged_chunks(td->data_chunk_id); //find the position in newest_chunk_log_order and update the log order
    new_log_chunk_cnt++;

    //if it is a new chunk update
	if(if_new_log_chnk==1)
		pl_log_cnt++;

	//ack the data chunk the finish of this update operation
	printf("pl_log_cnt=%d\n", pl_log_cnt);
		
	send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, recver_ip, UPDT_ACK_PORT, PARIX_UPDT_CMLT);
	printf("ack_info: logged_data=%d, recv_data_server=%s, port_num=%d\n", td->data_chunk_id, recver_ip, UPDT_ACK_PORT);
			
}

void pl_commit(TRANSMIT_DATA* td){

	//printf("td->op_type 		  =%d\n", td->op_type);
	//printf("td->stripe_id		  =%d\n", td->stripe_id);
	//printf("td->data_chunk_id	  =%d\n", td->data_chunk_id);
	//printf("td->updt_prty_id	  =%d\n", td->updt_prty_id);
	//printf("td->num_recv_chks_itn =%d\n", td->num_recv_chks_itn);
	//printf("td->num_recv_chks_prt =%d\n", td->num_recv_chks_prt);
	//printf("td->role			  =%d\n", td->role);
	//printf("td->port_num          =%d\n", td->port_num);
	//printf("td->next_ip 		  =%s\n", td->next_ip);
	printf("td->chunk_store_index =%d\n", td->chunk_store_index);

    int   global_chunk_id;
    int   its_stripe_id;
    int   updt_prty_id;

    char* data_delta=malloc(sizeof(char)*chunk_size);
	char* prty_delta=malloc(sizeof(char)*chunk_size);
	char* old_prty=(char*)malloc(sizeof(char)*chunk_size);
	char* new_prty=(char*)malloc(sizeof(char)*chunk_size);

    its_stripe_id=td->stripe_id;

	//read old data 
	read_old_data(old_prty, td->chunk_store_index);

	//read the parity delta 
	read_log_data(td->data_chunk_id, prty_delta, "pl_log_file");

	//calculate the new parity
	bitwiseXor(new_prty, old_prty, prty_delta, chunk_size);

	//write the new parity 
	write_new_data(new_prty, td->chunk_store_index);

	//send the ack to the client
	send_ack(its_stripe_id, -1, updt_prty_id, mt_svr_ip, td->port_num, CMMT_CMLT);

	//update the log table 
	evict_log_dt(newest_chunk_log_order, td->data_chunk_id);

    pl_log_cnt--;
	
	//update the log_file after all the parity_delta are committed
    //after the commit, reset the configurations
	if(pl_log_cnt==0){

		//check the file size 
		truncate("pl_log_file", 0);
		
		struct stat stat_info;
		stat("pl_log_file", &stat_info);
		//printf("AFTER Truncate: parix_log_file_size=%d\n", stat_info.st_size);

		stat("pl_log_old_file", &stat_info);
		//printf("AFTER Truncate: parix_log_old_file_size=%d\n", stat_info.st_size);
		
		new_log_chunk_cnt=0;

		}	
	

   free(old_prty);
   free(data_delta);
   free(new_prty);
   free(prty_delta);
	
}



int main(int argc, char** argv){

    int server_socket;
    int read_size;
    int recv_len;
    int connfd = 0;
	int local_node_id;

	char local_ip[ip_len];
	char sender_ip[ip_len];

    local_node_id=get_local_node_id();
	GetLocalIp(local_ip);

	if_gateway_open=GTWY_OPEN;


    //initial encoding coefficinets
    encoding_matrix=reed_sol_vandermonde_coding_matrix(data_chunks, num_chunks_in_stripe-data_chunks, w);

    //initial socket information
	server_socket=init_server_socket(UPDT_PORT);

    if(listen(server_socket,100) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

    //init the sender info
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    //init the hash_bucket
    new_log_chunk_cnt=0;
    memset(newest_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);

	//init the pl_log_cnt
	pl_log_cnt=0;

    //init the recv info
    TRANSMIT_DATA* td = malloc(sizeof(TRANSMIT_DATA));
    char* recv_buff = malloc(sizeof(TRANSMIT_DATA));

    while(1){
		
        connfd = accept(server_socket, (struct sockaddr*)&sender_addr,&length);

		memcpy(sender_ip, inet_ntoa(sender_addr.sin_addr), ip_len);
		printf("receive connection from %s\n", sender_ip);
		
        recv_len=0;
        while(recv_len < sizeof(TRANSMIT_DATA)){

            read_size=read(connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA));
            recv_len+=read_size;
            //printf("read_len=%d, expected_size=%lu\n", recv_len, sizeof(TRANSMIT_DATA));

        }

        printf("recv data succeeds\n");
        memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

        //if the process is to be terminated
		if(td->op_type==TERMINATE){

			printf("++++++ UPDATE FINISHES +++++++\n");
			close(connfd);

			break;

			}

		//if it is the gateway, then just forward the data to the destination node 
		if((strcmp(gateway_ip,local_ip)==0) && (if_gateway_open==1)){
			
			gateway_forward(td, sender_ip);
			close(connfd);
			continue;

			}

		if(td->op_type==DATA_UPDT)
			pl_update(td);

		else if(td->op_type==DATA_LOG){

			//check if it receives data from gateway
			if((strcmp(sender_ip, gateway_ip)==0) && (if_gateway_open==1))
				memcpy(sender_ip, td->next_ip, ip_len);

			printf("sender_ip=%s\n", sender_ip);

			pl_log(td, sender_ip, td->op_type);

			}

		else if(td->op_type==DATA_COMMIT)
			pl_commit(td);

		close(connfd);
		
    	}


	free(td);
	free(recv_buff);
	close(server_socket);

}

