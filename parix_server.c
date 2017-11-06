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

int   updt_strp_flag[max_updt_strps];
int   updt_cmlt_count;
int   need_old_dt_count;
int   parix_num_log_dt;

void* parix_send_updt_data(void* ptr){

   TRANSMIT_DATA td = *(TRANSMIT_DATA *)ptr;

   send_data(&td, td.sent_ip, td.port_num);

}


void parix_evict_log_dt(int* log_table, int logcal_data_id){

	int i,j; 
	int bucket_id;

	bucket_id=logcal_data_id%bucket_num;
	
    for(i=0; i<entry_per_bucket; i++){

        // if find the given_chunk_id, udpate its log order
        if(log_table[bucket_id*entry_per_bucket*2+2*i]==logcal_data_id)
            break;
		
    }

    //reset the bucket table after committing the logged data
	log_table[bucket_id*entry_per_bucket*2+2*i+1]=-1;

}

//if it is the first write to a data chunk, then parix will ask the data node to send the old version of the data and log it for later parity updates
int parix_update_loged_chunks(int given_chunk_id, int* log_table, int op_type){

    int bucket_id;
    int i,j;
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

    printf("\n");

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

    //read the data from the log_file
    if(mark_flag==LOG_NEW)
        fd=open("parix_log_file", O_RDONLY);

	else if(mark_flag==LOG_OLD)
		fd=open("parix_log_old_file", O_RDONLY);

	lseek(fd, log_order*chunk_size, SEEK_SET);
	ret=read(fd, log_data, chunk_size);
	if(ret<chunk_size){

	   perror("read_newest_log_data error!\n");
	   exit(1);
	}

    close(fd);

}

void* parix_recv_ack_process(void* ptr){

	//printf("recv_ack_process works:\n");

	RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;
	int its_chunk_id;
	int its_prty_id;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

    //receive data
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA));
        recv_len+=read_size;

    }

	//printf("parix_para_read_ack finishes!\n");

    //copy the data
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

	if(td->op_type==PARIX_UPDT_CMLT)
		updt_cmlt_count++;

	else if(td->op_type==PARIX_NEED_OLD_DT)
		need_old_dt_count++;

	free(td);
	free(recv_buff);

}



//receive data by using multiple threads
void parix_para_recv_data(int stripe_id, int num_recv_chnks, int port_num, int recv_type){

    //printf("para_recv_data starts:\n");

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

    //initial socket information

	server_socket=init_server_socket(port_num);

    if(listen(server_socket,max_connctn) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	RECV_PROCESS_DATA* rpd=(RECV_PROCESS_DATA *)malloc(sizeof(RECV_PROCESS_DATA)*num_recv_chnks);
	memset(rpd, 0, sizeof(RECV_PROCESS_DATA)*num_recv_chnks);


    while(1){

        connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
        //printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));

        rpd[index].connfd=connfd[index];
        rpd[index].recv_id=index;

		if(recv_type==ACK)
			pthread_create(&recv_data_thread[index], NULL, parix_recv_ack_process, (void *)(rpd+index));
		
        index++;

		//printf("index=%d, num_recv_chnks=%d\n", index, num_recv_chnks);

        if(index>=num_recv_chnks){
			//printf("index>=num_recv_chunks\n");
			break;
        	}

    }

    for(i=0; i<num_recv_chnks; i++){
		//printf("waiting join: i=%d, num_recv_chnks=%d\n", i, num_recv_chnks);
        pthread_join(recv_data_thread[i], NULL);
    	}

	for(i=0; i<num_recv_chnks; i++)
		close(connfd[i]);

    free(rpd);
	free(connfd);
	//close the sockets 
	close(server_socket);
	//printf("recv_completes\n");

}


//in-place write the new data
int parix_write_new_data(char* write_buff, int store_index){

   int fd; 
   int ret; 

   fd=open("data_file", O_RDWR);
   lseek(fd, store_index*chunk_size, SEEK_SET);
   ret=write(fd, write_buff, chunk_size);
   if(ret!=chunk_size)
	   printf("write data error!\n");
   
   //printf("write new data succeeds\n");
   
   close(fd);
   
   return store_index;

}


int parix_read_old_data(char* read_buff, int store_index){

    int ret;
    int fd;

    fd=open("data_file", O_RDONLY);
    lseek(fd, store_index*chunk_size, SEEK_SET);
    ret=read(fd, read_buff, chunk_size);
    if(ret!=chunk_size)
        printf("read data error!\n");

    //printf("read old data succeeds\n");

    close(fd);

    return store_index;

}


void parix_para_send_dt_prty(TRANSMIT_DATA* td, int op_type){

   int j; 
   int its_prty_node_id;

   //brodcast the new data to m parity chunks
   TRANSMIT_DATA* td_mt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*(num_chunks_in_stripe-data_chunks));
   
   pthread_t parix_updt_thread[num_chunks_in_stripe-data_chunks];
   memset(parix_updt_thread, 0, sizeof(parix_updt_thread));

   for(j=0; j<num_chunks_in_stripe-data_chunks; j++){
   
	   //init td structure
	   td_mt[j].op_type=op_type; 
	   td_mt[j].data_chunk_id=td->data_chunk_id;
	   td_mt[j].stripe_id=td->data_chunk_id/data_chunks;
	   td_mt[j].num_recv_chks_itn=-1;
	   td_mt[j].num_recv_chks_prt=-1;
	   td_mt[j].port_num=UPDT_PORT;
	   td_mt[j].role=-1; 
	   td_mt[j].updt_prty_id=j;
	   memcpy(td_mt[j].buff, td->buff, chunk_size);
   
	   its_prty_node_id=td->updt_prty_nd_id[j]; 
	   //printf("td->stripe_id=%d, td->updt_prty_nd_id[%d]=%d\n", td->stripe_id, j, its_prty_node_id);
	   memcpy(td_mt[j].sent_ip, node_ip_set[its_prty_node_id], ip_len);

	   //printf("its_prty_nd_ip=%s\n", td_mt[j].sent_ip);
   
	   pthread_create(&parix_updt_thread[j], NULL, parix_send_updt_data, td_mt+j);
   
	   }
   
   //join the threads
   for(j=0; j<num_chunks_in_stripe-data_chunks; j++)
	   pthread_join(parix_updt_thread[j], NULL);

   free(td_mt);



}

//it is performed at the data chunk by 1) in-place writing the new data; 2) sending the new data to m parity chunks
void parix_update(TRANSMIT_DATA* td){

   int j; 
   int global_chunk_id;
   
   parix_para_send_dt_prty(td, PARIX_LGWT);

   //listen the ack from parity chunks 
   updt_cmlt_count=0;
   need_old_dt_count=0;
   parix_para_recv_data(td->stripe_id, num_chunks_in_stripe-data_chunks, UPDT_ACK_PORT, ACK);

   //printf("need_old_dt_count=%d\n", need_old_dt_count);

   if(need_old_dt_count==(num_chunks_in_stripe-data_chunks)){

	  char* old_data=(char*)malloc(sizeof(char)*chunk_size);
	  parix_read_old_data(old_data, td->chunk_store_index);

	  memcpy(td->buff, old_data, sizeof(char)*chunk_size);
	  parix_para_send_dt_prty(td, PARIX_OLD);

	  need_old_dt_count=0;
	  parix_para_recv_data(td->stripe_id, num_chunks_in_stripe-data_chunks, UPDT_ACK_PORT, ACK);

	  free(old_data);
	  
   	}

   if(updt_cmlt_count!=(num_chunks_in_stripe-data_chunks)){

	  printf("update error! updt_cmlt_count=%d\n", updt_cmlt_count);
	  exit(1);

   	}

   //in-place update the new data 
   //printf("td->chunk_store_index=%d\n", td->chunk_store_index);
   global_chunk_id=td->stripe_id*num_chunks_in_stripe+td->data_chunk_id%data_chunks;
   parix_write_new_data(td->buff, td->chunk_store_index);

   //send ack to the client node
   send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, mt_svr_ip, UPDT_PORT, PARIX_UPDT_CMLT);
   
}


//it is performed at the parity chunk that log the new data in the log file
void parix_log_write(TRANSMIT_DATA* td, char* recver_ip, int op_type){

    int ret;
	int i;
	int if_need_old_dt;
	int fd;

    //log the new data
	if(op_type==PARIX_LGWT){

		//specify fd at the bottom of the file
		fd=open("parix_log_file", O_RDWR|O_CREAT, 0644);
		lseek(fd, 0, SEEK_END);
		
		ret=write(fd, td->buff, chunk_size);
		if(ret!=chunk_size){
			printf("write_parix_log_file_error! ret=%d\n",ret);
			perror(ret);
		}

		if_need_old_dt=parix_update_loged_chunks(td->data_chunk_id, newest_chunk_log_order, td->op_type); 
		new_log_chunk_cnt++;
		
		//locate the receiver_id 
		for(i=0; i<total_nodes_num; i++)
			if(strcmp(recver_ip, node_ip_set[i])==0){
		
				//printf("receiver_id=%d\n", i);
				break;
		
				}
		
		//ack the data chunk the finish of this update operation
		//printf("if_need_old_dt=%d\n", if_need_old_dt);
		
		if(if_need_old_dt==0){
			send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, recver_ip, UPDT_ACK_PORT, PARIX_UPDT_CMLT);
			//printf("ack_info: logged_data=%d, recv_data_server=%s, port_num=%d\n", td->data_chunk_id, recver_ip, UPDT_ACK_PORT);
			}
		
		else if (if_need_old_dt==1){
			send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, recver_ip, UPDT_ACK_PORT, PARIX_NEED_OLD_DT);
			//printf("ack_info: logged_data=%d, recv_data_server=%s, port_num=%d\n", td->data_chunk_id, recver_ip, UPDT_ACK_PORT);
			}

		}

	else if (op_type==PARIX_OLD){

		//specify fd at the bottom of the file
		fd=open("parix_log_old_file", O_RDWR|O_CREAT, 0644);
		lseek(fd, 0, SEEK_END);

		ret=write(fd, td->buff, chunk_size);
		if(ret!=chunk_size){
			//printf("write_parix_log_file_error! ret=%d\n",ret);
			perror(ret);
		}

		parix_update_loged_chunks(td->data_chunk_id, old_chunk_log_order, td->op_type); 
		old_log_chunk_cnt++;

		//locate the receiver_id 
		for(i=0; i<total_nodes_num; i++)
			if(strcmp(recver_ip, node_ip_set[i])==0){
		
				//printf("receiver_id=%d\n", i);
				break;
		
				}

		//send ack 
		send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, recver_ip, UPDT_ACK_PORT, PARIX_UPDT_CMLT);

		}

	close(fd);
	
}

void parix_commit(TRANSMIT_DATA* td){

	//printf("td->op_type 		  =%d\n", td->op_type);
	//printf("td->stripe_id		  =%d\n", td->stripe_id);
	//printf("td->data_chunk_id	  =%d\n", td->data_chunk_id);
	//printf("td->updt_prty_id	  =%d\n", td->updt_prty_id);
	//printf("td->num_recv_chks_itn =%d\n", td->num_recv_chks_itn);
	//printf("td->num_recv_chks_prt =%d\n", td->num_recv_chks_prt);
	//printf("td->role			  =%d\n", td->role);
	//printf("td->port_num          =%d\n", td->port_num);
	//printf("td->next_ip 		  =%s\n", td->next_ip);
	//printf("td->chunk_store_index =%d\n", td->chunk_store_index);

    int   local_chunk_id, global_chunk_id;
    int   its_stripe_id;
    int   updt_prty_id;
	int   i;
	int   dt_chnk_id;

    char* data_delta=malloc(sizeof(char)*chunk_size);
	char* prty_delta=malloc(sizeof(char)*chunk_size);
	char* old_data=(char*)malloc(sizeof(char)*chunk_size);
	char* new_data=(char*)malloc(sizeof(char)*chunk_size);

    its_stripe_id=td->stripe_id;


	if(td->role==PARITY){

       updt_prty_id=td->updt_prty_id;
       global_chunk_id=its_stripe_id*num_chunks_in_stripe+data_chunks+updt_prty_id;
				
	   //for the chunk in each stripe, read the old and newest data from log file
	   char* log_data=(char*)malloc(sizeof(char)*chunk_size*td->num_recv_chks_prt);
	   char* old_prty_data=(char*)malloc(sizeof(char)*chunk_size);
	   char* new_prty_data=(char*)malloc(sizeof(char)*chunk_size);

	   parix_read_old_data(old_prty_data, td->chunk_store_index);

	   //locate the logged old and new data from different log files
	   parix_read_log_data(old_data, td->data_chunk_id, old_chunk_log_order, LOG_OLD);
	   parix_read_log_data(new_data, td->data_chunk_id, newest_chunk_log_order, LOG_NEW);

	   //calculate data delta
	   bitwiseXor(data_delta, old_data, new_data, chunk_size);
	   encode_data(data_delta, prty_delta, td->data_chunk_id, td->updt_prty_id);

	   //obtain the new parity
	   bitwiseXor(new_prty_data, old_prty_data, prty_delta, chunk_size);
	   	
	   //flush the data 
	   flush_new_data(td->stripe_id, new_prty_data, global_chunk_id, td->chunk_store_index);
	   
	   //send back ack
	   //printf("ready_send_ack:\n");
	   send_ack(its_stripe_id, -1, updt_prty_id, mt_svr_ip, td->port_num, CMMT_CMLT);
       //printf("======= (stripe_id=%d, parity_id=%d) COMPLETELY COMMIT !!!\n ========", its_stripe_id, updt_prty_id);

	   //update the number of distinct data 
	   parix_num_log_dt--;

	   printf("parix_num_log_dt=%d\n", parix_num_log_dt);

	   //update the log_table
	   parix_evict_log_dt(newest_chunk_log_order, td->data_chunk_id);
	   parix_evict_log_dt(old_chunk_log_order, td->data_chunk_id);

	   free(log_data);
	   free(old_prty_data);
	   free(new_prty_data);

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
    int local_node_id;
    int connfd = 0;
	int i;

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
	old_log_chunk_cnt=0;
    memset(newest_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);
	memset(old_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);
    memset(updt_strp_flag, -1, sizeof(int)*max_updt_strps);

	//init the parix_num_log_dt
	parix_num_log_dt=0;

    num_updt_strps=0;

    //init the recv info
    TRANSMIT_DATA* td = malloc(sizeof(TRANSMIT_DATA));
    char* recv_buff = malloc(sizeof(TRANSMIT_DATA));

    read_chunk_map("chunk_map");
    local_node_id=get_local_node_id();

    while(1){
        connfd = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
        //printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));
        //printf("expected_recv_data_size=%lu\n", sizeof(TRANSMIT_DATA));

        // we should differentiate the accepted data in the new data or the data delta for aggregation
        // judge it the data comes from client (log-write) or servers (partial encoding)

        //read data from socket
        recv_len=0;

        //read the first op_type
        while(recv_len < sizeof(TRANSMIT_DATA)){

            read_size=read(connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA));
            recv_len+=read_size;
            //printf("read_len=%d, expected_size=%lu\n", recv_len, sizeof(TRANSMIT_DATA));

        }

        //printf("recv data succeeds\n");
        memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

		if(td->op_type==PARIX_UPDT)
			parix_update(td);

		else if(td->op_type==PARIX_LGWT)
			parix_log_write(td, inet_ntoa(sender_addr.sin_addr), td->op_type);

		else if(td->op_type==PARIX_OLD)
			parix_log_write(td, inet_ntoa(sender_addr.sin_addr), td->op_type);

		else if(td->op_type==PARIX_CMMT)
			parix_commit(td);

		close(connfd);
		
    	}


	free(td);
	free(recv_buff);
	close(server_socket);

}

