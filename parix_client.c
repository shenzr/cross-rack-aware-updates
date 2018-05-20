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

int cross_rack_updt_traffic;

<<<<<<< HEAD
/*
 * This function performs update using parix approach
*/
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void parix_update(META_INFO* md){
	
   int node_id;
   int j;
   int rack_id;
   int prty_rack_id;

<<<<<<< HEAD
   // init transmit data 
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
   char* recv_buff=(char *)malloc(sizeof(TRANSMIT_DATA));

   td->send_size=sizeof(TRANSMIT_DATA);
   td->op_type=PARIX_UPDT;
   td->num_recv_chks_itn=-1;
   td->num_recv_chks_prt=-1;
   td->port_num=UPDT_PORT;
   td->prty_delta_app_role=-1;
   td->updt_prty_id=-1;
   strcpy(td->from_ip,"");

   td->data_chunk_id=md->data_chunk_id;
   td->stripe_id=md->stripe_id;
   td->chunk_store_index=md->chunk_store_index;
   
   node_id=get_node_id(md->next_ip);
   rack_id=get_rack_id(node_id);

<<<<<<< HEAD
   // init its associated parity node id
=======
   //init its associated parity node id
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   for(j=0; j<num_chunks_in_stripe-data_chunks; j++){
   	
	  td->updt_prty_nd_id[j]=md->updt_prty_nd_id[j];
	  td->updt_prty_store_index[j]=md->updt_prty_store_index[j];
	  
	  prty_rack_id=get_rack_id(td->updt_prty_nd_id[j]);

	  if(prty_rack_id!=rack_id){

        //if it is the first update, parix should also send the old data to the parity node with the new data
		if(md->if_first_update==1)
			cross_rack_updt_traffic+=2;

		else 
			cross_rack_updt_traffic++;

	  	}
   	}

<<<<<<< HEAD
   // we generate random data to simulate the new data
   gene_radm_buff(td->buff, chunk_size);

   // copy the ip address of the storage node from metadata info
   memcpy(td->next_ip, md->next_ip, ip_len);

   //printf("Send new data to: ");
   //print_amazon_vm_info(td->next_ip);
   
   // if there is a gateway server, then send the data to the gateway first; else, directly send the data to the destined storage server
=======
   gene_radm_buff(td->buff, chunk_size);
   memcpy(td->next_ip, md->next_ip, ip_len);

   printf("Send new data to: ");
   print_amazon_vm_info(td->next_ip);

   //printf("next_ip=%s\n", td->next_ip);
   //send the data to the gateway first
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   if(if_gateway_open==1)
   	send_data(td, gateway_ip, td->port_num, NULL, NULL, UPDT_DATA);
   else 
   	send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

<<<<<<< HEAD
   // listen ack
=======
   //listen ack
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
   listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_PORT, PARIX_UPDT_CMLT);

   free(td);
   free(ack);
   free(recv_buff);

}

<<<<<<< HEAD
/*
 * This function extracts each update operation from a given trace and performs the update 
*/
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void parix_read_trace(char *trace_name){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
<<<<<<< HEAD
        printf("open file failed\n");
        exit(1);
    }
	
	// the format of a MSR Trace: [timestamp, workload_name, volumn_id, op_type, access_offset, operated_size, duration_time]
=======
        //printf("open file failed\n");
        exit(0);
    }

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

<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    int i;
    int access_start_chunk, access_end_chunk;
    int access_chunk_num;
    int ret;
<<<<<<< HEAD

	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

=======
    int count;

	double acc_updt_time=0;


	META_INFO* metadata=(META_INFO*)malloc(sizeof(META_INFO));

	struct timeval bg_tm, ed_tm;
	struct timeval ud_bg_tm, ud_ed_tm;

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
<<<<<<< HEAD

    memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));
	cross_rack_updt_traffic=0;
	
	while(fgets(operation, sizeof(operation), fp)){

        // partition the operation
=======
    count=0;

    memset(mark_updt_stripes_tab, -1, sizeof(int)*(max_updt_strps+num_tlrt_strp)*(data_chunks+1));
	cross_rack_updt_traffic=0;

	gettimeofday(&bg_tm, NULL);

	while(fgets(operation, sizeof(operation), fp)){

	    gettimeofday(&ud_bg_tm, NULL);

        printf("count=%d\n",count);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
		new_strtok(operation,divider,volumn_id);
		new_strtok(operation,divider,op_type);
		new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
		new_strtok(operation,divider,usetime);

<<<<<<< HEAD
        if((ret=strcmp(op_type, "Read"))==0)
            continue;

        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);
        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));
        access_chunk_num += access_end_chunk - access_start_chunk + 1;
		
		//for each new write, send the logical_chunk_id to the metadata server first and then perform the update
=======

        if((ret=strcmp(op_type, "Read"))==0)
            continue;

		count++;

		if(count%1==0)
			printf("count=%d, update_time=%lf\n", count, acc_updt_time);


        //printf("\ncount=%d, op_type=%s, offset=%s, size=%s\n", count, op_type, offset, size);

        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));
        access_chunk_num += access_end_chunk - access_start_chunk + 1;

        //printf("access_start_stripe=%d, access_end_stripe=%d\n", access_start_stripe, access_end_stripe);
        //printf("access_start_chunk=%d, access_end_chunk=%d\n", access_start_chunk, access_end_chunk);
		
		//for each new write, send the data to its desined chunk 
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
		for(i=access_start_chunk; i<=access_end_chunk; i++){

			connect_metaserv(i, metadata);
			parix_update(metadata);
<<<<<<< HEAD

			}
		}  

	fclose(fp);
	free(metadata);

=======
			//printf("parix_update succeed!\n");

			}
		
		gettimeofday(&ud_ed_tm, NULL);
		acc_updt_time+=ud_ed_tm.tv_sec-ud_bg_tm.tv_sec+(ud_ed_tm.tv_usec-ud_bg_tm.tv_usec)*1.0/1000000;

		printf("update finish\n");


		}  

	gettimeofday(&ed_tm, NULL);
	
	printf("%s, PARIX: count=%d, cross_rack_updt_traffic=%d, time=%lf\n", trace_name, count, cross_rack_updt_traffic, ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

	//printf("TRACE REPLAY FINISHES!\n");
	fclose(fp);
	free(metadata);

/*
	//send a finish cmd to all the servers
	TRANSMIT_DATA* tmnt_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));

	tmnt_td->op_type=TERMINATE;

	for(i=0; i<total_nodes_num; i++)
		send_data(tmnt_td, node_ip_set[i], UPDT_PORT);

	send_data(tmnt_td, gateway_ip, UPDT_PORT);

	free(tmnt_td);
*/
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
}


int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

<<<<<<< HEAD
=======
	cross_rack_updt_traffic=0;

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	printf("Trace: %s\n", argv[1]);
	parix_read_trace(argv[1]);

    return 0;
}

