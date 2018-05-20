#define _GNU_SOURCE

<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>
#include <arpa/inet.h>

#include "common.h"
#include "config.h"

<<<<<<< HEAD
int cross_rack_updt_traffic; 

/*
 * This function performs update using baseline delta-update approach
*/
=======
int cross_rack_updt_traffic; //it counts the number of chunks in cross-rack data transfers

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void baseline_update(META_INFO* md){

   int node_id;
   int j;
   int base=node_num_per_rack;
   int recv_rack_id;

<<<<<<< HEAD
   // init transmit data 
   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
=======
   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   td->send_size=sizeof(TRANSMIT_DATA);
   td->op_type=DATA_UPDT; 
   td->port_num=UPDT_PORT;
   td->num_recv_chks_itn=-1;
   td->num_recv_chks_prt=-1;
   td->updt_prty_id=-1;
<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   td->data_chunk_id=md->data_chunk_id%data_chunks;
   td->stripe_id=md->stripe_id;
   td->chunk_store_index=md->chunk_store_index;
   memcpy(td->next_ip, md->next_ip, ip_len);

   for(j=0; j<num_chunks_in_stripe-data_chunks; j++){

	td->updt_prty_nd_id[j]=md->updt_prty_nd_id[j];
	td->updt_prty_store_index[j]=md->updt_prty_store_index[j];

   	}

<<<<<<< HEAD
   // we generate random data to simulate the new data
   gene_radm_buff(td->buff,chunk_size);

   // print the info of destined storage server 
=======
   struct timeval begin_time, end_time;
   gettimeofday(&begin_time, NULL);

   gene_radm_buff(td->buff,chunk_size);

   gettimeofday(&end_time, NULL);
   //printf("gen_rand_time=%lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   gettimeofday(&begin_time, NULL);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   node_id=get_node_id(td->next_ip);
   recv_rack_id=get_rack_id(node_id);
   printf("Send new data to Region-%s, Node-%d\n", region_name[recv_rack_id], node_id%base);

<<<<<<< HEAD
   // send the data to the storage server
   if(if_gateway_open==1)
   	send_data(td, gateway_ip, td->port_num, NULL, NULL, UPDT_DATA);
   else
   	send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

   // listen ack
=======
   //printf("send_data_to %s\n", td->next_ip);
   send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

   gettimeofday(&end_time, NULL);
   printf("send_data_time=%lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   gettimeofday(&begin_time, NULL);

   //listen ack
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
   char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

   listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, UPDT_CMLT);

<<<<<<< HEAD
=======
   gettimeofday(&end_time, NULL);
   printf("listen_ack_time=%lf\n\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   free(td);
   free(ack);
   free(recv_buff);
   
}



<<<<<<< HEAD
/* 
 * This function extracts each update operation from a given trace and performs the update 
*/
=======
//read a trace
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void baseline_read_trace(char *trace_name){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(0);
    }

<<<<<<< HEAD
    // the format of a MSR Trace: [timestamp, workload_name, volumn_id, op_type, access_offset, operated_size, duration_time]
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
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
<<<<<<< HEAD
=======
    int count;
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
<<<<<<< HEAD
=======
    count=0;
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

	struct timeval time1_bg, time1_ed;
	struct timeval bg_tm, ed_tm;
	gettimeofday(&bg_tm, NULL);

	cross_rack_updt_traffic=0;

	double update_time=0;
<<<<<<< HEAD

	// read every operation from the trace
=======
	
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    while(fgets(operation, sizeof(operation), fp)) {

		gettimeofday(&time1_bg, NULL);

<<<<<<< HEAD
        // partition the operation
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
		new_strtok(operation,divider,volumn_id);
		new_strtok(operation,divider,op_type);
		new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
		new_strtok(operation,divider,usetime);

        if((ret=strcmp(op_type, "Read"))==0)
            continue;

<<<<<<< HEAD
        // transform the char type to int type, and calculate the operated chunks
        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);
=======
        count++;

		if(count%100==0)
			printf("count=%d, update_time=%lf\n", count, update_time);

        printf("\n\ncount=%d\n", count);

        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));
        access_chunk_num += access_end_chunk - access_start_chunk + 1;
		
<<<<<<< HEAD
        // connect metadata server to initialize "metadata" sturcture and perform update for each updated chunk
        for(i=access_start_chunk; i<=access_end_chunk; i++){
			connect_metaserv(i, metadata);
			baseline_update(metadata);
=======
        //perform update for each updated chunk
        for(i=access_start_chunk; i<=access_end_chunk; i++){
			connect_metaserv(i, metadata);
			baseline_update(metadata);
            //printf("log_write succeeds\n");
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
        }

		gettimeofday(&time1_ed, NULL);
		
		update_time+=time1_ed.tv_sec-time1_bg.tv_sec+(time1_ed.tv_usec-time1_bg.tv_usec)*1.0/1000000;

    }

    fclose(fp);
	free(metadata);

	gettimeofday(&ed_tm, NULL);
<<<<<<< HEAD
	printf("%s BASELINE: cross_rack_updt_traffic=%d, time=%lf\n", trace_name, cross_rack_updt_traffic, ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
=======
	printf("%s BASELINE: count=%d, cross_rack_updt_traffic=%d, time=%lf\n", trace_name, count, cross_rack_updt_traffic, ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

}


<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

	printf("Trace: %s\n", argv[1]);
<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    baseline_read_trace(argv[1]);

    return 0;
}


