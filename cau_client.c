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

#define UPPBND   9999

int cross_rack_updt_traffic;

void cau_update(META_INFO* md){

   int node_id;
   int j;
   int rack_id;
   int prty_rack_id;
   
   struct timeval begin_time, end_time;

   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));

   td->send_size=sizeof(TRANSMIT_DATA);
   td->op_type=DATA_UPDT;
   td->port_num=UPDT_PORT;

   td->data_chunk_id=md->data_chunk_id;
   td->stripe_id=md->stripe_id;

   for(j=0; j<num_chunks_in_stripe-data_chunks; j++)
   	td->updt_prty_nd_id[j]=md->updt_prty_nd_id[j];

   //fill the parity info
   gene_radm_buff(td->buff, chunk_size);
   memcpy(td->next_ip, md->next_ip, ip_len);
   
   //printf("Send new data to: ");
   //print_amazon_vm_info(td->next_ip);

   gettimeofday(&begin_time, NULL);

   //send the data
   send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

   gettimeofday(&end_time, NULL);
   //printf("send_data_time=%lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   gettimeofday(&begin_time, NULL);

   //listen the ack info
   ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
   char* recv_buff=(char*)malloc(sizeof(ACK_DATA));
   
   listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, LOG_CMLT);

   gettimeofday(&end_time, NULL);
   //printf("listen_ack=%lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

   free(td);
   free(recv_buff);
   free(ack);

}

//read a trace
void cau_read_trace(char *trace_name){

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

    int i,j;
    int k;
    int access_start_chunk, access_end_chunk;
    int access_chunk_num;
    int total_num_req;
    int ret;
    int num_rcrd_strp;
    int access_start_stripe, access_end_stripe;
    int count;
    int temp_start_chunk, temp_end_chunk;
	int updt_chnk_cnt;
	double update_time;
	
    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
    count=0;
    total_num_req=0;
    num_rcrd_strp=0;
	updt_chnk_cnt=0;

	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

    memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));
	cross_rack_updt_traffic=0;
	
	struct timeval ud_bg_tm, ud_ed_tm;
	struct timeval time1_bg, time1_ed;
	struct timeval cm_bg_tm, cm_ed_tm;
	
	gettimeofday(&ud_bg_tm, NULL);

	update_time=0;

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

		if(count%100==0){
			printf("\ncount=%d, accumulated_update_time=%lf\n", count, update_time);
			update_time=0;
			}

        //printf("\n\n\ncount=%d, op_type=%s, offset=%s, size=%s\n", count, op_type, offset, size);

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

		updt_chnk_cnt+=access_end_chunk-access_start_chunk+1;

        //printf("access_start_stripe=%d, access_end_stripe=%d\n", access_start_stripe, access_end_stripe);
        //printf("access_start_chunk=%d, access_end_chunk=%d\n", access_start_chunk, access_end_chunk);

        //perform log_write for each updated chunk
        for(i=access_start_chunk; i<=access_end_chunk; i++){
			//printf("chunk_id=%d\n", i);
			connect_metaserv(i, metadata);
			cau_update(metadata);
            //printf("log_write succeeds\n");

        }

		gettimeofday(&time1_ed, NULL);

		update_time+=time1_ed.tv_sec-time1_bg.tv_sec+(time1_ed.tv_usec-time1_bg.tv_usec)*1.0/1000000;

      
    }

	//printf("cross_rack_updt_traffic=%d\n", cross_rack_updt_traff);

	//when the trace is completely perform, commit the data deltas

	//printf("update_time=%lf\n", update_time);

	//printf("num_rcrd_strp=%d, max_updt_strps=%d\n", num_rcrd_strp, max_updt_strps);
	gettimeofday(&ud_ed_tm, NULL);
	printf("%s, CAU-%d: updt_time=%lf\n", trace_name, cau_num_rplc, ud_ed_tm.tv_sec-ud_bg_tm.tv_sec+(ud_ed_tm.tv_usec-ud_bg_tm.tv_usec)*1.0/1000000);
    fclose(fp);
	free(metadata);
	
}


int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

	cross_rack_updt_traffic=0;
	
	printf("Trace: %s\n", argv[1]);

	struct timeval begin_time, end_time;

	gettimeofday(&begin_time, NULL);
    cau_read_trace(argv[1]);
	gettimeofday(&end_time, NULL);

	printf("cau_time=%.2lf\n", end_time.tv_sec-begin_time.tv_sec+(end_time.tv_usec-begin_time.tv_usec)*1.0/1000000);

    return 0;
}

