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

#define max_log_chunk_cnt 200
#define bucket_num 20
#define entry_per_bucket max_log_chunk_cnt/bucket_num
#define max_num_store_chunks 1024*1024*100 //it denotes the maximum number of data chunks allowed to stored on a node
#define SERVER_PORT 1111

int newest_chunk_log_order[max_log_chunk_cnt*2];//it records the mapping between the newest version of chunk_id and the logged order;
//chunk_id stored are sored in ascending orde
int num_updt_strps; //it denotes the number of stripes updated on a node
int updt_strp_flag[max_updt_strps];


// a hash function to check the integrity of received data
unsigned int RSHash(char* str, unsigned int len)
{
    unsigned int b    = 378551;
    unsigned int a    = 63689;
    unsigned int hash = 0;
    unsigned int i    = 0;

    for(i = 0; i < len; str++, i++)
    {
        hash = hash * a + (*str);
        a    = a * b;
    }

    return hash;
}


// given the stripe_id and the parity chunk id, this stripe is to return the node id where the parity chunk to be updated resides on
int locate_prty_node_id(int stripe_id, int prty_chunk_id){

    return global_chunk_map[stripe_id*num_chunks_in_stripe+prty_chunk_id];

}


// write the received data in a log file
// update the offset of the wrtten chunk
void log_write(TRANSMIT_DATA* td){

	//printf("enter log_file func:\n");
	
    int ret;

    //specify fd at the bottom of the file
    int fd=open("log_file", O_RDWR|O_CREAT, 0644);
    lseek(fd, 0, SEEK_END);

    ret=write(fd, td->buff, chunk_size);
    if(ret!=chunk_size){
        printf("write_log_file_error! ret=%d\n",ret);
        perror(ret);
    }

    update_loged_chunks(td->data_chunk_id); //find the position in newest_chunk_log_order and update the log order
    new_log_chunk_cnt++;

    close(fd);
}


void read_old_data(char* read_buff, int store_index){

    int ret;
    int fd;

    //read the old data from data_file

    //printf("store_index=%d\n", store_index);

    fd=open("data_file", O_RDONLY);
    lseek(fd, store_index*chunk_size, SEEK_SET);
    ret=read(fd, read_buff, chunk_size);
    if(ret!=chunk_size)
        printf("read data error!\n");

    //printf("read old data succeeds\n");

    close(fd);

}


// we update the data chunks stripe by stripe.
void read_cal_data_delta(int stripe_id, char* data_delta, int local_chunk_id, int global_chunk_id, int store_index){

    int i;
    //get the host ip_addr

    int bucket_id;
    int log_order;
	int ret;

    //read the old data from data file and new data from log file
    //locate the log_order
    bucket_id=local_chunk_id%bucket_num;

    for(i=0; i<entry_per_bucket; i++){

        if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]==local_chunk_id){
            log_order=newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i+1];
			break;
        	}
    }

	if(i>=entry_per_bucket){

		printf("ERROR: log not found!\n");
		exit(1);

		}

    //printf("log_order=%d\n",log_order);

    //read the newest data from the log_file
    char *log_data=malloc(sizeof(char)*chunk_size);
    int fd=open("log_file", O_RDONLY);
    lseek(fd, log_order*chunk_size, SEEK_SET);
    ret=read(fd, log_data, chunk_size);
	if(ret<chunk_size){

		printf("read_log_data error!\n");
		exit(1);
		
		}

    close(fd);

    //printf("read_log_data success\n");

    char* ori_data=malloc(sizeof(char)*chunk_size);

    read_old_data(ori_data, store_index);
    bitwiseXor(data_delta, ori_data, log_data, chunk_size);

    //printf("read_data_delta succeed\n");

    free(log_data);
    free(ori_data);
}


void cau_dtnd_log_updt(TRANSMIT_DATA* td){

   //printf("data_update:\n");

   int i;
   int its_stripe_id;
   int log_prty_id;
   
   //validate the data
   //printf("recv: td->data_chunk_id=%d\n", td->data_chunk_id);
   //printf("recv: td->op_type=%d\n", td->op_type);

   //update the recorded stripe list
   its_stripe_id=td->stripe_id;

   for(i=0; i<num_updt_strps; i++){

	 //printf("i=%d, num_updt_strps=%d\n",i,num_updt_strps);
	 if(updt_strp_flag[i]==its_stripe_id)
		 break;
   }

   if(i>=num_updt_strps){

	 updt_strp_flag[i]=its_stripe_id;
	 num_updt_strps++;
   }

   //case#1:if it is new data, then log it
   log_write(td);

   td->op_type=DATA_LOG;//change the op_type only 
   td->chunk_store_index=-1;

   send_data(td, td->next_ip, td->port_num);

   //listen ack 
   TRANSMIT_DATA* ntf_dt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
   char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));
   
   listen_ack(ntf_dt, recv_buff, td->stripe_id, td->data_chunk_id, -1, LOG_ACK_PORT, LOG_CMLT);

   //send ack to client node
   send_ack(td->stripe_id, td->data_chunk_id, -1, mt_svr_ip, UPDT_ACK_PORT, LOG_CMLT);

   free(ntf_dt);
   free(recv_buff);

}



void cau_commit(TRANSMIT_DATA* td){


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

    //init the sent info
    TRANSMIT_DATA* ped = malloc(sizeof(TRANSMIT_DATA));
    TRANSMIT_DATA* delta = malloc(sizeof(TRANSMIT_DATA));			

    char* data_delta=malloc(sizeof(char)*chunk_size);
    char* pse_data_delta=malloc(sizeof(char)*chunk_size);
    char* old_prty=malloc(sizeof(char)*chunk_size);

    int   local_chunk_id, global_chunk_id;
    int   its_stripe_id;
    int   updt_prty_id;
	int   count_flag;
    int   index;

	count_flag=0;

    its_stripe_id=td->stripe_id;

    //if it is the first commit operation
	if(count_flag==0)
        num_updt_strps=num_updt_strps*(num_chunks_in_stripe-data_chunks);//it needs to update m parity chunks for each stripe

	//printf("BEFORE COMMIT: count_flag=%d, num_updt_strps=%d\n", count_flag, num_updt_strps);

	count_flag++;

    //if it is leaf node, read the data, calculate the data delta, and send the delta to the internal node
    if(td->role==LEAF){

       //printf("it is LEAF\n");

	   //init settings
       local_chunk_id=td->data_chunk_id;
       global_chunk_id=its_stripe_id*num_chunks_in_stripe+local_chunk_id%data_chunks; // this global id is used to read the old data from data file
       //printf("local_chunk_id=%d, global_chunk_id=%d\n", local_chunk_id, global_chunk_id);

	   //printf("td->chunk_store_index=%d\n", td->chunk_store_index);

       read_cal_data_delta(its_stripe_id, data_delta, local_chunk_id, global_chunk_id, td->chunk_store_index);
       encode_data(data_delta, pse_data_delta, global_chunk_id, td->updt_prty_id);

       delta->op_type=DELTA;
       delta->stripe_id=td->stripe_id;
       delta->data_chunk_id=td->data_chunk_id;
       delta->updt_prty_id=td->updt_prty_id;
       delta->num_recv_chks_itn=td->num_recv_chks_itn;
       delta->num_recv_chks_prt=td->num_recv_chks_prt;
       delta->role=-1;
       memset(delta->next_ip,'0',50);
       memcpy(delta->buff, data_delta, chunk_size);

       send_data(delta, td->next_ip, td->port_num);

      }


     //if it is a internal node, then receive data by using multiple threads, aggregate the data, and send it to the parity node
     else if(td->role==INTERNAL){

        //printf("It is INTERNAL\n");

        local_chunk_id=td->data_chunk_id;
        global_chunk_id=its_stripe_id*num_chunks_in_stripe+local_chunk_id%data_chunks; // this global id is used to read the old data from data file

		//printf("td->chunk_store_index=%d\n", td->chunk_store_index);

        read_cal_data_delta(its_stripe_id, data_delta, local_chunk_id, global_chunk_id, td->chunk_store_index);
		encode_data(data_delta, pse_data_delta, global_chunk_id, td->updt_prty_id);
        //set up the socket with the specified port num and receive the data by using multiple threads
        if(td->num_recv_chks_itn>=1){
			para_recv_data(its_stripe_id, td->num_recv_chks_itn, td->port_num, 1);
            aggregate_data(data_delta, td->num_recv_chks_itn);
         }

        ped->op_type=DATA_PE;
        ped->stripe_id=td->stripe_id;
        ped->data_chunk_id=td->data_chunk_id;
        ped->updt_prty_id=td->updt_prty_id;
        ped->num_recv_chks_itn=td->num_recv_chks_itn;
        ped->num_recv_chks_prt=td->num_recv_chks_prt;
        ped->role=-1;
	    ped->port_num=td->port_num;
        memset(ped->next_ip,'0',50);
        memcpy(ped->buff, data_delta, chunk_size);

        send_data(ped, td->next_ip, ped->port_num);

	    //printf("\n");

      }


      //if it is a parity node, then receive pse data, aggregate them with the old parity, and store them
      else if(td->role==PARITY){

         //printf("It is PARITY\n");

         updt_prty_id=td->updt_prty_id;
         global_chunk_id=its_stripe_id*num_chunks_in_stripe+data_chunks+updt_prty_id;
         //printf("stripe_id=%d, updt_prty_id=%d\n", its_stripe_id, updt_prty_id);
         read_old_data(old_prty, td->chunk_store_index);
         //printf("store_index=%d\n",  td->chunk_store_index);

         if(td->num_recv_chks_prt>=1){
            para_recv_data(its_stripe_id, td->num_recv_chks_prt, td->port_num, 1);
            aggregate_data(old_prty, td->num_recv_chks_prt);
           }

		 //printf("begin_flush_data:\n");
				
         flush_new_data(its_stripe_id, old_prty, global_chunk_id, td->chunk_store_index); 

		 //notify the client the parity is commtted successfully, the por num is 1111

		 //printf("ready_send_ack:\n");
				
		 TRANSMIT_DATA* ntf_ack_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
				
		 ntf_ack_td->op_type=CMMT_CMLT;
		 ntf_ack_td->stripe_id=its_stripe_id;
		 ntf_ack_td->role=PARITY;
		 ntf_ack_td->updt_prty_id=updt_prty_id;
				
		 ntf_ack_td->data_chunk_id=-1;
		 ntf_ack_td->num_recv_chks_itn=-1;
		 ntf_ack_td->num_recv_chks_prt=-1;
		 memset(ntf_ack_td->next_ip, '0', 50);
		 memset(ntf_ack_td->buff, 0, chunk_size*sizeof(char)); 
		 send_data(ntf_ack_td, mt_svr_ip, td->port_num);

		 free(ntf_ack_td);
			
         //printf("======= (stripe_id=%d, parity_id=%d) COMPLETELY COMMIT !!!\n ========", its_stripe_id, updt_prty_id);

       }

	if(td->role==LEAF || td->role==INTERNAL){

        num_updt_strps--;

		//printf("CMT_CMPLT: num_updt_strps=%d\n", num_updt_strps);

        //truncate the log file after all the dirty stripes are committed
        if(num_updt_strps==0){
					
           //truncate("log_file", 0);

		   //check the file size 
		   struct stat stat_info;
		   stat("log_file", &stat_info);
		   //printf("AFTER Truncate: log_file_size=%d\n", stat_info.st_size);

           //reset the default settings
           memset(newest_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);
           memset(updt_strp_flag, -1, sizeof(int)*max_updt_strps);
		   count_flag=0;
		   new_log_chunk_cnt=0;
         }
				
	}


	free(data_delta);
    free(pse_data_delta);
	free(old_prty);
    free(ped);
    free(delta);

}

void cau_send_cold_data(TRANSMIT_DATA* td){

	//read the cold data 
	char* cold_buff=(char*)malloc(sizeof(char)*chunk_size);

	//printf("recv_out_chnk_store_index=%d\n", td->chunk_store_index);

	read_old_data(cold_buff, td->chunk_store_index);

	memcpy(td->buff, cold_buff, sizeof(char)*chunk_size);
	td->role=OUT_CHNK;

	send_data(td, mt_svr_ip, td->port_num);

	free(cold_buff);
	
}


void cau_write_hot_data(TRANSMIT_DATA* td){

	flush_new_data(td->stripe_id, td->buff, -1, td->chunk_store_index);

}


int main(int argc, char** argv){


    signal(SIGPIPE,SIG_IGN);
    int server_socket;
    int i;
    int read_size;
    int recv_len;
    int connfd = 0;
	int local_node_id;

	local_node_id=get_local_node_id();

    //initial encoding coefficinets
    encoding_matrix=reed_sol_vandermonde_coding_matrix(data_chunks, num_chunks_in_stripe-data_chunks, w);

    //initial socket information

	server_socket=init_server_socket(SERVER_PORT);

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
    memset(updt_strp_flag, -1, sizeof(int)*max_updt_strps);

    num_updt_strps=0;

    //init the recv info
    TRANSMIT_DATA* td = malloc(sizeof(TRANSMIT_DATA));
    char *recv_buff = malloc(sizeof(TRANSMIT_DATA));

    while(1){
        connfd = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
        //printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));
        //printf("expected_recv_data_size=%lu\n", sizeof(TRANSMIT_DATA));

        // we should differentiate the accepted data in the new data or the data delta for aggregation
        // judge it the data comes from client (log-write) or servers (partial encoding)

        //read data from socket
        recv_len=0;

        while(recv_len < sizeof(TRANSMIT_DATA)){

            read_size=read(connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA));
            recv_len+=read_size;
            //printf("read_len=%d, expected_size=%lu\n", recv_len, sizeof(TRANSMIT_DATA));

        }

        //printf("recv data succeeds\n");
        memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

        if(td->op_type==DATA_UPDT)
			cau_dtnd_log_updt(td);

		else if(td->op_type==DATA_LOG){//this is performed at the parity chunk side 

		    //printf("prty_side: log chunk_id=%d\n", td->data_chunk_id);
			log_write(td);
			send_ack(td->stripe_id, td->data_chunk_id, -1, inet_ntoa(sender_addr.sin_addr), LOG_ACK_PORT, LOG_CMLT);
			
			}
			
        else if(td->op_type==DATA_COMMIT)
			cau_commit(td);

		else if(td->op_type==CMD_MVMNT)
			cau_send_cold_data(td);

		else if(td->op_type==DATA_MVMNT)
			cau_write_hot_data(td);

    }

    free(td);
    free(recv_buff);
	close(server_socket);
	close(connfd);

    return 0;

}
