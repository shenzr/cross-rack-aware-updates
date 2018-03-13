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

int cross_rack_updt_traffic; //it counts the number of chunks in cross-rack data transfers

void baseline_update(META_INFO* md){

   int node_id;
   int j;
   int rack_id;
   int temp_rack_id;
   int base=node_num_per_rack;
   int local_chunk_id;
   int recv_rack_id;

   //init td
   struct timeval bg_tm, ed_tm;

   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));

   local_chunk_id=md->data_chunk_id;
   td->send_size=sizeof(TRANSMIT_DATA);
   td->op_type=DATA_UPDT; 
   td->port_num=UPDT_PORT;
   td->num_recv_chks_itn=-1;
   td->num_recv_chks_prt=-1;
   td->updt_prty_id=-1;

   td->data_chunk_id=md->data_chunk_id%data_chunks;
   td->stripe_id=md->stripe_id;
   td->chunk_store_index=md->chunk_store_index;
   memcpy(td->next_ip, md->next_ip, ip_len);

   for(j=0; j<num_chunks_in_stripe-data_chunks; j++){

	td->updt_prty_nd_id[j]=md->updt_prty_nd_id[j];
	td->updt_prty_store_index[j]=md->updt_prty_store_index[j];

   	}

   struct timeval begin_time, end_time;
   gettimeofday(&begin_time, NULL);

   gene_radm_buff(td->buff,chunk_size);

   gettimeofday(&end_time, NULL);
   //printf("gen_rand_time=%lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   gettimeofday(&begin_time, NULL);

   node_id=get_node_id(td->next_ip);
   recv_rack_id=get_rack_id(node_id);
   printf("Send new data to Region-%s, Node-%d\n", region_name[recv_rack_id], node_id%base);

   //printf("send_data_to %s\n", td->next_ip);
   send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

   gettimeofday(&end_time, NULL);
   printf("send_data_time=%lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   gettimeofday(&begin_time, NULL);

   //listen ack
   ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
   char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

   listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, UPDT_CMLT);

   gettimeofday(&end_time, NULL);
   printf("listen_ack_time=%lf\n\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   free(td);
   free(ack);
   free(recv_buff);
   
}



//read a trace
void baseline_read_trace(char *trace_name){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(0);
    }


    char operation[150];
	char time_stamp[50];
	char workload_name[10];
	char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
	char usetime[10];
    char divider=',';

    int i;
    int access_start_chunk, access_end_chunk;
    int access_chunk_num;
    int ret;
    int access_start_stripe, access_end_stripe;
    int count;

	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
    count=0;

	struct timeval time1_bg, time1_ed;
	struct timeval bg_tm, ed_tm;
	gettimeofday(&bg_tm, NULL);

	cross_rack_updt_traffic=0;

	double update_time=0;
	
    while(fgets(operation, sizeof(operation), fp)) {

		gettimeofday(&time1_bg, NULL);

        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
		new_strtok(operation,divider,volumn_id);
		new_strtok(operation,divider,op_type);
		new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
		new_strtok(operation,divider,usetime);

        if((ret=strcmp(op_type, "Read"))==0)
            continue;

        count++;

		if(count%100==0)
			printf("count=%d, update_time=%lf\n", count, update_time);

        printf("\n\ncount=%d\n", count);

        // printf("cur_rcd_idx=%d\n",cur_rcd_idx);
        // analyze the access pattern
        // if it is accessed in the same timestamp

        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));
        access_chunk_num += access_end_chunk - access_start_chunk + 1;

        access_start_stripe=access_start_chunk/data_chunks;
        access_end_stripe=access_end_chunk/data_chunks;

        //perform update for each updated chunk
        for(i=access_start_chunk; i<=access_end_chunk; i++){
			connect_metaserv(i, metadata);
			baseline_update(metadata);
            //printf("log_write succeeds\n");
        }

		gettimeofday(&time1_ed, NULL);
		
		update_time+=time1_ed.tv_sec-time1_bg.tv_sec+(time1_ed.tv_usec-time1_bg.tv_usec)*1.0/1000000;

    }

    fclose(fp);
	free(metadata);

	gettimeofday(&ed_tm, NULL);
	printf("%s BASELINE: count=%d, cross_rack_updt_traffic=%d, time=%lf\n", trace_name, count, cross_rack_updt_traffic, ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}



int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

	printf("Trace: %s\n", argv[1]);

    baseline_read_trace(argv[1]);

    return 0;
}


