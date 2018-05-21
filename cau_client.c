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

/*
 * This function performs the update operation and fills the sent structure 
 */
void cau_update(META_INFO* md){

	int j;

	// initialize update data structure
	TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
	td->send_size=sizeof(TRANSMIT_DATA);
	td->op_type=DATA_UPDT;
	td->port_num=UPDT_PORT;
	td->data_chunk_id=md->data_chunk_id;
	td->stripe_id=md->stripe_id;

	for(j=0; j<num_chunks_in_stripe-data_chunks; j++)
		td->updt_prty_nd_id[j]=md->updt_prty_nd_id[j];

	// fill the updated info by randomly generating a data buffer 
	gene_radm_buff(td->buff, chunk_size); 

	// send the data
	memcpy(td->next_ip, md->next_ip, ip_len);
	send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

	// listen the ack info
	ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
	char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

	listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_ACK_PORT, LOG_CMLT);

	free(td);
	free(recv_buff);
	free(ack);

}

/*
 * This function replys the update operations in a trace file
 */ 
void cau_read_trace(char *trace_name){

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
	int ret;

	long long *size_int;
	long long *offset_int;
	long long a,b;
	a=0LL;
	b=0LL;
	offset_int=&a;
	size_int=&b;

	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

	memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));

	// read every operation from the trace
	while(fgets(operation, sizeof(operation), fp)){

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

	fclose(fp);
	free(metadata);

}


int main(int argc, char** argv){

	if(argc!=2){
		printf("./client_size trace_name!\n");
		exit(0);
	}

	printf("Trace: %s\n", argv[1]);
	cau_read_trace(argv[1]);

	return 0;
}

