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
#include <sys/time.h>

#include "common.h"
#include "config.h"

#define UPPBND   9999

<<<<<<< HEAD
/*
 * This function performs the update operation and fills the sent structure 
*/
=======
/* perform the update operation and fill the sent structure */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void cau_update(META_INFO* md){

   int j;

<<<<<<< HEAD
   // initialize update data structure
   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
=======
   struct timeval bg_tm, ed_tm;

   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   td->send_size=sizeof(TRANSMIT_DATA);
   td->op_type=DATA_UPDT;
   td->port_num=UPDT_PORT;
   td->data_chunk_id=md->data_chunk_id;
   td->stripe_id=md->stripe_id;

   for(j=0; j<num_chunks_in_stripe-data_chunks; j++)
   	td->updt_prty_nd_id[j]=md->updt_prty_nd_id[j];

<<<<<<< HEAD
   // fill the updated info by randomly generating a data buffer 
   gene_radm_buff(td->buff, chunk_size);
   
   // send the data
   memcpy(td->next_ip, md->next_ip, ip_len);
   send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

   // listen the ack info
   ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
   char* recv_buff=(char*)malloc(sizeof(ACK_DATA));
   
   listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, LOG_CMLT);
=======
   //fill the updated info by randomly generating a data buffer 
   gettimeofday(&bg_tm, NULL);
   gene_radm_buff(td->buff, chunk_size);
   memcpy(td->next_ip, md->next_ip, ip_len);
   gettimeofday(&ed_tm, NULL);
   printf("--gen_rand_buff_time=%.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

   //send the data
   gettimeofday(&bg_tm, NULL);
   send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);
   gettimeofday(&ed_tm, NULL);
   print_amazon_vm_info(td->next_ip);
   printf("--send_time=%.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

   //listen the ack info
   ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
   char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

   gettimeofday(&bg_tm, NULL);
   listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, LOG_CMLT);
   gettimeofday(&ed_tm, NULL);
   printf("--listen_ack_time=%.2lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

   free(td);
   free(recv_buff);
   free(ack);

}

<<<<<<< HEAD
/*
 * This function replys the update operations in a trace file
*/ 
void cau_read_trace(char *trace_name){

=======
/* read a trace file */
void cau_read_trace(char *trace_name){

    //read the data from a csv file
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
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
    int ret;
<<<<<<< HEAD
=======
	int req_cnt;
	double updt_tm;

	struct timeval bg_tm, ed_tm;
	struct timeval test_bg_tm, test_ed_tm;
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	
    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;

<<<<<<< HEAD
=======
	req_cnt=0;

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

    memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));

<<<<<<< HEAD
	// read every operation from the trace
    while(fgets(operation, sizeof(operation), fp)){
=======
    while(fgets(operation, sizeof(operation), fp)) {
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

        // read the attribute information of a request
        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
		new_strtok(operation,divider,volumn_id);
		new_strtok(operation,divider,op_type);
		new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
		new_strtok(operation,divider,usetime);

        if((ret=strcmp(op_type, "Read"))==0)
            continue;
		
        // read the offset and operation size 
<<<<<<< HEAD
        // get the range of operated data chunks 
        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);
        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));

        // for each operated data chunk, connect the metadata server and then perform the update
        for(i=access_start_chunk; i<=access_end_chunk; i++){
			
			connect_metaserv(i, metadata);				
			cau_update(metadata);

        }
    }
=======
        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        // get the range of operated data chunks 
        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));

		gettimeofday(&bg_tm, NULL);

        // for each operated data chunk, connect the metadata server and then perform the update
        for(i=access_start_chunk; i<=access_end_chunk; i++){

            printf("req_cnt=%d\n", req_cnt);
		    gettimeofday(&test_bg_tm, NULL);	
			connect_metaserv(i, metadata);
			gettimeofday(&test_ed_tm, NULL);
		    printf("-connect_mds_time=%.2lf\n", test_ed_tm.tv_sec-test_bg_tm.tv_sec+(test_ed_tm.tv_usec-test_bg_tm.tv_usec)*1.0/1000000);

		    gettimeofday(&test_bg_tm, NULL);				
			cau_update(metadata);
			gettimeofday(&test_ed_tm, NULL);
		    printf("-update_time=%.2lf\n\n", test_ed_tm.tv_sec-test_bg_tm.tv_sec+(test_ed_tm.tv_usec-test_bg_tm.tv_usec)*1.0/1000000);

        }

		req_cnt++;

		gettimeofday(&ed_tm, NULL);
		updt_tm+=ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000;

		if(req_cnt%100==0)
			printf("req_cnt=%d, updt_tm=%.2lf\n", req_cnt, updt_tm);
		

    }

	printf("Trace: %s, CAU, updt_tm=%.2lf\n", updt_tm);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	
    fclose(fp);
	free(metadata);
	
}


int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }
	
	printf("Trace: %s\n", argv[1]);
<<<<<<< HEAD
    cau_read_trace(argv[1]);
=======

	struct timeval begin_time, end_time;

	gettimeofday(&begin_time, NULL);
    cau_read_trace(argv[1]);
	gettimeofday(&end_time, NULL);

	printf("cau_time=%.2lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

    return 0;
}

