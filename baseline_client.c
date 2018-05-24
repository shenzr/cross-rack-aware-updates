#define _GNU_SOURCE

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

/*
 * This function performs update using baseline delta-update approach
 */
void baseline_update(META_INFO* md){

    int j;

    // init transmit data 
    TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
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

    // we generate random data to simulate the new data
    gene_radm_buff(td->buff,chunk_size);

    // send the data to the storage server
    if(GTWY_OPEN)
        send_data(td, gateway_ip, td->port_num, NULL, NULL, UPDT_DATA);
    else
        send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

    // listen ack
    ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
    char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

    listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, UPDT_CMLT);

    free(td);
    free(ack);
    free(recv_buff);

}


/* 
 * This function extracts each update operation from a given trace and performs the update 
 */
void baseline_read_trace(char *trace_name){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(0);
    }

    // the format of a MSR Trace: [timestamp, workload_name, volumn_id, op_type, access_offset, operated_size, duration_time]
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
	int updt_req_cnt;
    int ret;

    META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
	updt_req_cnt=0;

    // read every operation from the trace
    while(fgets(operation, sizeof(operation), fp)) {

        // partition the operation
        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
        new_strtok(operation,divider,usetime);

        if((ret=strcmp(op_type, "Read"))==0)
            continue;

		updt_req_cnt++;

		if(updt_req_cnt%500==0)
			printf("%d update request finish \n", updt_req_cnt);

        // transform the char type to int type, and calculate the operated chunks
        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);
        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));
        access_chunk_num += access_end_chunk - access_start_chunk + 1;

        // connect metadata server to initialize "metadata" sturcture and perform update for each updated chunk
        for(i=access_start_chunk; i<=access_end_chunk; i++){
            connect_metaserv(i, metadata);
            baseline_update(metadata);
        }

    }

    fclose(fp);
    free(metadata);

}


int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

    printf("Trace: %s\n", argv[1]);
    baseline_read_trace(argv[1]);
	
	printf("Baseline: Trace-%s replay finishes\n", argv[1]);

    return 0;
}


