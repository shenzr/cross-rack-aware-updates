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

int parix_num_log_dt;

//if it is the first write to a data chunk, then parix will ask the data node to send the old version of the data and log it for later parity updates
int parix_update_loged_chunks(int given_chunk_id, int* log_table, int op_type){

    int bucket_id;
    int i;

	int if_need_old_dt;

    bucket_id=given_chunk_id%bucket_num;
	
    // if the bucket is full
    if(log_table[bucket_id*entry_per_bucket*2+2*(entry_per_bucket-1)]>0){

        printf("Error! bucket_%d is full!\n", bucket_id);
        exit(0);

    }

    //scan the entries in that bucket
    if(op_type==PARIX_LGWT)
		if_need_old_dt=0;
	
    for(i=0; i<entry_per_bucket; i++){

        // if find the given_chunk_id, udpate its log order
        if(log_table[bucket_id*entry_per_bucket*2+2*i]==given_chunk_id)
            break;

        // if reach the initialized ones, then it needs the old data
        if(log_table[bucket_id*entry_per_bucket*2+2*i]==-1){

            if(op_type==PARIX_LGWT){
				if_need_old_dt=1;
				parix_num_log_dt++;
            	}
			
            break;
        	}
    }

    // record the given_chunk_id
    log_table[bucket_id*entry_per_bucket*2+2*i]=given_chunk_id;

	if(op_type==PARIX_LGWT)
		log_table[bucket_id*entry_per_bucket*2+2*i+1]=new_log_chunk_cnt;
	else if(op_type==PARIX_OLD)
		log_table[bucket_id*entry_per_bucket*2+2*i+1]=old_log_chunk_cnt;

/*
	if(op_type==PARIX_LGWT)
		printf("new_log_bucket_record:\n");
	else 
		printf("old_log_bucket_record:\n");
	
    for(i=0; i<entry_per_bucket; i++){

        for(j=0; j<bucket_num; j++)
            printf("%d ", log_table[j*entry_per_bucket*2+i*2]);

        printf("\n");
    }

    //printf("\n");

	if(op_type==PARIX_LGWT)
		printf("new_log_bucket_count:\n");
	else 
		printf("old_log_bucket_count:\n");

    for(i=0; i<entry_per_bucket; i++){

        for(j=0; j<bucket_num; j++)
            printf("%d ", log_table[j*entry_per_bucket*2+i*2+1]);

        printf("\n");
    }
*/
	return if_need_old_dt;

}

void parix_read_log_data(char* log_data, int local_chunk_id, int* log_table, int mark_flag){

	//printf("parix_read_log_data works:\n");

    int ret;
	int bucket_id;
	int i; 
	int log_order;
	int fd;

    //read the old data from data file and new data from log file
    //locate the log_order
    bucket_id=local_chunk_id%bucket_num;

    for(i=0; i<entry_per_bucket; i++){

        if(log_table[bucket_id*entry_per_bucket*2+2*i]==local_chunk_id){
            log_order=log_table[bucket_id*entry_per_bucket*2+2*i+1];
			break;
        	}
    }

	if(i>=entry_per_bucket){

		printf("No found local_chunk_id: local_chunk_id=%d, entry_per_bucket=%d\n", local_chunk_id, entry_per_bucket);
		exit(1);

		}

	//printf("mark_flag=%d, log_order=%d\n", mark_flag, log_order);

	char* tmp_buff;
	posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);

    //read the data from the log_file
    if(mark_flag==LOG_NEW)
        fd=open("parix_log_file", O_RDONLY);

	else if(mark_flag==LOG_OLD)
		fd=open("parix_log_old_file", O_RDONLY);

	lseek(fd, log_order*chunk_size, SEEK_SET);
	ret=read(fd, tmp_buff, chunk_size);
	if(ret<chunk_size){

	   perror("read_newest_log_data error!\n");
	   printf("old_log_chunk_cnt=%d\n", old_log_chunk_cnt);
	   exit(1);
	}

	memcpy(log_data, tmp_buff, sizeof(char)*chunk_size);

    close(fd);
	free(tmp_buff);

}

//it is performed at the data chunk by 1) in-place writing the new data; 2) sending the new data to m parity chunks
void parix_server_update(TRANSMIT_DATA* td){

   int sum_cmplt;
   int sum_need;
   
   para_send_dt_prty(td, PARIX_LGWT, num_chunks_in_stripe-data_chunks, td->port_num);

   //listen the ack from parity chunks 
   memset(updt_cmlt_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));
   memset(need_old_dt_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));
   para_recv_ack(td->stripe_id, num_chunks_in_stripe-data_chunks, UPDT_ACK_PORT);

   sum_need=sum_array(num_chunks_in_stripe-data_chunks, need_old_dt_count);

   //if the parity chunks need the old data, then resend the old data to them
   if(sum_need==(num_chunks_in_stripe-data_chunks)){

	  char* old_data=(char*)malloc(sizeof(char)*chunk_size);
	  read_old_data(old_data, td->chunk_store_index);

	  memcpy(td->buff, old_data, sizeof(char)*chunk_size);
	  para_send_dt_prty(td, PARIX_OLD, num_chunks_in_stripe-data_chunks, td->port_num);

	  memset(need_old_dt_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));
	  para_recv_ack(td->stripe_id, num_chunks_in_stripe-data_chunks, UPDT_ACK_PORT);

	  free(old_data);
	  
   	}

   sum_cmplt=sum_array(num_chunks_in_stripe-data_chunks, updt_cmlt_count);

   if(sum_cmplt!=(num_chunks_in_stripe-data_chunks)){

	  printf("update error! sum_cmplt=%d\n", sum_cmplt);
	  exit(1);

   	}

   //in-place update the new data 
   write_new_data(td->buff, td->chunk_store_index);

   //send ack to the client node
   send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, client_ip, UPDT_PORT, PARIX_UPDT_CMLT);
   
}


//it is performed at the parity chunk that log the new data in the log file
void parix_log_write(TRANSMIT_DATA* td, char* sender_ip, int op_type){


	int if_need_old_dt;

	if((strcmp(sender_ip, gateway_ip)==0) && (if_gateway_open==1))
		memcpy(td->next_ip, td->from_ip, ip_len);

	else 
		memcpy(td->next_ip, sender_ip, ip_len);

    //log the new data
	if(op_type==PARIX_LGWT){

		//specify fd at the bottom of the file
		log_write("parix_log_file", td);

		if_need_old_dt=parix_update_loged_chunks(td->data_chunk_id, newest_chunk_log_order, td->op_type); 
		new_log_chunk_cnt++;
		
		//ack the data chunk the finish of this update operation
		printf("if_need_old_dt=%d\n", if_need_old_dt);
		
		if(if_need_old_dt==0){
			send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, td->next_ip, UPDT_ACK_PORT, PARIX_UPDT_CMLT);
			printf("ack_info: logged_data=%d, recv_data_server=%s, port_num=%d\n", td->data_chunk_id, td->next_ip, UPDT_ACK_PORT);
			}
		
		else if (if_need_old_dt==1){
			send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, td->next_ip, UPDT_ACK_PORT, PARIX_NEED_OLD_DT);
			printf("ack_info: logged_data=%d, recv_data_server=%s, port_num=%d\n", td->data_chunk_id, td->next_ip, UPDT_ACK_PORT);
			}

		}

	else if (op_type==PARIX_OLD){

		printf("op_type==PARIX_OLD\n");

		//specify fd at the bottom of the file
		log_write("parix_log_old_file", td);
		parix_update_loged_chunks(td->data_chunk_id, old_chunk_log_order, td->op_type); 
		old_log_chunk_cnt++;

		//send ack 
		send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, td->next_ip, UPDT_ACK_PORT, PARIX_UPDT_CMLT);

		}
	
}

void parix_server_commit(CMD_DATA* cmd){

	//printf("PARIX Commit Starts:\n");

/*
    printf("\nparix_commit:\n");
	printf("td->op_type 		  =%d\n", td->op_type);
	printf("td->stripe_id		  =%d\n", td->stripe_id);
	printf("td->data_chunk_id	  =%d\n", td->data_chunk_id);
	//printf("td->updt_prty_id	  =%d\n", td->updt_prty_id);
	//printf("td->num_recv_chks_itn =%d\n", td->num_recv_chks_itn);
	//printf("td->num_recv_chks_prt =%d\n", td->num_recv_chks_prt);
	//printf("td->role			  =%d\n", td->role);
	printf("td->port_num          =%d\n", td->port_num);
	printf("td->from_ip 		  =%s\n", td->from_ip);
	printf("td->next_ip 		  =%s\n", td->next_ip);
	printf("td->chunk_store_index =%d\n", td->chunk_store_index);

	printf("update_data_chunks_in_the_stripe:\n");
	print_array(1, data_chunks, td->parix_updt_data_id);
*/

    int   global_chunk_id;
    int   its_stripe_id;
    int   updt_prty_id;
	int   local_chunk_id;
	int   i;
	int   ret;

    char* data_delta=malloc(sizeof(char)*chunk_size);
	char* prty_delta=malloc(sizeof(char)*chunk_size);
	char* old_data=(char*)malloc(sizeof(char)*chunk_size);
	char* new_data=(char*)malloc(sizeof(char)*chunk_size);

    its_stripe_id=cmd->stripe_id;


	if(cmd->prty_delta_app_role==PARITY){

       updt_prty_id=cmd->updt_prty_id;
       global_chunk_id=its_stripe_id*num_chunks_in_stripe+data_chunks+updt_prty_id;

	   char* old_prty_data=(char*)malloc(sizeof(char)*chunk_size);
	   char* new_prty_data=(char*)malloc(sizeof(char)*chunk_size);

	   read_old_data(old_prty_data, cmd->chunk_store_index);

	   for(i=0; i<data_chunks; i++){

          //if the data is not updated then break
		  if(cmd->parix_updt_data_id[i]==-1)
		  	continue;

	      local_chunk_id=cmd->stripe_id*data_chunks+i;

	      //locate the logged old and new data from different log files
	      //printf("local_chunk_id=%d\n", local_chunk_id);
	      parix_read_log_data(old_data, local_chunk_id, old_chunk_log_order, LOG_OLD);
	      parix_read_log_data(new_data, local_chunk_id, newest_chunk_log_order, LOG_NEW);

	      //calculate data delta
	      bitwiseXor(data_delta, old_data, new_data, chunk_size);
	      encode_data(data_delta, prty_delta, i, cmd->updt_prty_id);

	      //obtain the new parity
	      bitwiseXor(new_prty_data, old_prty_data, prty_delta, chunk_size);

		  //copy the new parity data to the old parity data for the next round of update
		  memcpy(old_prty_data, new_prty_data, chunk_size);

		  //update the log_table
		  evict_log_dt(newest_chunk_log_order, local_chunk_id);
		  evict_log_dt(old_chunk_log_order, local_chunk_id);

		  //update the record num
		  parix_num_log_dt--;

	   	}
	   	
	   //flush the data 
	   flush_new_data(cmd->stripe_id, old_prty_data, global_chunk_id, cmd->chunk_store_index);
	   
	   //send back ack
	   //printf("ready_send_ack:\n");
	   send_ack(its_stripe_id, -1, updt_prty_id, mt_svr_ip, CMMT_PORT, CMMT_CMLT);

	   //printf("parix_num_log_dt=%d\n", parix_num_log_dt);

	   free(old_prty_data);
	   free(new_prty_data);

	}


    //after the commit, reset the configurations
	if(parix_num_log_dt==0){

		//check the file size 
		ret=truncate("parix_log_file", 0);
		if(ret!=0){
			perror("truncate_parix_log_file error!\n");
			exit(1);
			}
		
		ret=truncate("parix_log_old_file", 0);
		if(ret!=0){
			perror("truncate_parix_old_file error!\n");
			exit(1);
			}
		

		struct stat stat_info;
		stat("parix_log_file", &stat_info);
		printf("AFTER Truncate: parix_log_file_size=%d\n", (int)(stat_info.st_size));

		stat("parix_log_old_file", &stat_info);
		printf("AFTER Truncate: parix_log_old_file_size=%d\n", (int)(stat_info.st_size));

		//printf("\n\n");

		//reset the global tables
		new_log_chunk_cnt=0;
		old_log_chunk_cnt=0;

		}

   free(old_data);
   free(data_delta);
   free(new_data);
   free(prty_delta);
	
}



int main(int argc, char** argv){

    int server_socket;
    int read_size;
    int recv_len;
    int connfd = 0;
	int recv_data_type;
	int send_size;

	char local_ip[ip_len];
	char* sender_ip;

	if_gateway_open=GTWY_OPEN;

    //initial encoding coefficinets
    encoding_matrix=reed_sol_vandermonde_coding_matrix(data_chunks, num_chunks_in_stripe-data_chunks, w);

    //initial socket information
	server_socket=init_server_socket(UPDT_PORT);

    if(listen(server_socket,20) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

    //init the hash_bucket
    new_log_chunk_cnt=0;
	old_log_chunk_cnt=0;
    memset(newest_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);
	memset(old_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);

	//init the parix_num_log_dt
	parix_num_log_dt=0;
    num_updt_strps=0;

    //init the recv info
    TRANSMIT_DATA* td = (TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
	ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
	CMD_DATA* cmd=(CMD_DATA*)malloc(sizeof(CMD_DATA));

    char* recv_buff = (char*)malloc(sizeof(TRANSMIT_DATA));
	char* recv_head = (char*)malloc(head_size);

    while(1){

		struct sockaddr_in sender_addr;
		socklen_t length=sizeof(sender_addr);

        printf("before accept:\n");
        connfd = accept(server_socket, (struct sockaddr*)&sender_addr, &length);
		printf("connfd=%d\n", connfd);

		if(connfd<0){

			perror("Accpet fails!\n");

			}

		send_size=-1;
		sender_ip=inet_ntoa(sender_addr.sin_addr);
		
		printf("receive connection from: ");
		if(strcmp(sender_ip, mt_svr_ip)==0)
			printf("MetaData Server\n");
		else if(strcmp(sender_ip, client_ip)==0)
			printf("Client \n");
		else
			print_amazon_vm_info(sender_ip);
		
        recv_len=0;

		//first read a part of data to determine the size of transmitted data
		while(recv_len < head_size){
			read_size=read(connfd, recv_head+recv_len, head_size-recv_len);
			recv_len+=read_size;
			//printf("recv_len=%d, head_size=%lu\n", recv_len, head_size);
			}
		
		memcpy(&send_size, recv_head, sizeof(int));
		memcpy(recv_buff, recv_head, read_size);

		printf("recv_len=%d\n", recv_len);	
		
        while(recv_len < send_size){

            read_size=read(connfd, recv_buff+recv_len, send_size-recv_len);
            recv_len+=read_size;
            //printf("read_len=%d, expected_size=%lu\n", recv_len, send_size);

        }

		recv_data_type=-1;

        //if it contains the updated data
		if(send_size==sizeof(TRANSMIT_DATA)){
			
			recv_data_type=UPDT_DATA;
        	memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));
			
			}

		//else it is an ack info
		else if(send_size==sizeof(ACK_DATA)){

			recv_data_type=ACK_INFO;
			memcpy(ack, recv_buff, sizeof(ACK_DATA));
			
			}

		else if(send_size==sizeof(CMD_DATA)){

			recv_data_type=CMD_INFO;
			memcpy(cmd, recv_buff, sizeof(CMD_DATA));

			}

		else{

			printf("ERR: unrecognized_send_size!\n");
			exit(1);
			
			}
/*
        //if the process is to be terminated
		if(td->op_type==TERMINATE){

			printf("++++++ UPDATE FINISHES +++++++\n");
			close(connfd);

			break;

			}
*/
		//if it is the gateway, then just forward the data to the destination node 
		if((strcmp(gateway_ip,local_ip)==0) && (if_gateway_open==1)){

			if(recv_data_type==UPDT_DATA)
				gateway_forward_updt_data(td, sender_ip);

			else if (recv_data_type==ACK_INFO)
				gateway_forward_ack_info(ack);

			else if (recv_data_type==CMD_INFO)
				gateway_forward_cmd_data(cmd);

			close(connfd);
			continue;

			}
		
		if(td->op_type==PARIX_UPDT && recv_data_type==UPDT_DATA){

			printf("Recv New Data from MetaData Server\n");
			
			parix_server_update(td);

			printf("Update Finishes\n");
			
			}

		else if((td->op_type==PARIX_LGWT || td->op_type==PARIX_OLD) && recv_data_type==UPDT_DATA){

			printf("Log Data from: \n");
			print_amazon_vm_info(sender_ip);

			parix_log_write(td, sender_ip, td->op_type);

			}

		else if(cmd->op_type==PARIX_CMMT && recv_data_type==CMD_INFO)
			parix_server_commit(cmd);

		close(connfd);

		printf("connection close\n");
		
    	}


	free(td);
	free(recv_buff);
	free(recv_head);
	free(ack);
	free(cmd);
	close(server_socket);

}

