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


int updt_strp_flag[stripe_num]; // it records if a stripe is updated
int mark_updt_stripes_tab[max_updt_strps*(data_chunks+1)]; //it records the updated data chunks and their stripes, the data_chunk column stores the data, while the data_chunk-th column store the stripe_id

void* parix_send_cmd_process(void* ptr){

	TRANSMIT_DATA pcd=*(TRANSMIT_DATA*)ptr;

	TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
    td->op_type=pcd.op_type;
    td->stripe_id=pcd.stripe_id;
    td->data_chunk_id=pcd.data_chunk_id;
    td->updt_prty_id=pcd.updt_prty_id;
    td->num_recv_chks_itn=-1;
    td->num_recv_chks_prt=pcd.num_recv_chks_prt;
    td->role=pcd.role;
	td->port_num=pcd.port_num;
	td->chunk_store_index=pcd.chunk_store_index;
	
	memcpy(td->next_ip, pcd.next_ip, 50);
	
	int client_socket; 
	client_socket=init_client_socket(); 
	
    //set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    //printf("connected_server_ip=%s, port_num=%d\n", pcd.sent_ip, UPDT_PORT);

    if(inet_aton(pcd.sent_ip,&server_addr.sin_addr) == 0)
    {
        //printf("Server IP Address Error!\n");
        exit(1);
    }
    server_addr.sin_port = htons(UPDT_PORT);
    socklen_t server_addr_length = sizeof(server_addr);

    //connect server
    while(connect(client_socket,(struct sockaddr*)&server_addr, server_addr_length) < 0);

    //printf("connect success!\n");

    //init sent_buff
    char *sent_buff=malloc(sizeof(TRANSMIT_DATA));

    memcpy(sent_buff, td, sizeof(TRANSMIT_DATA));

    int sent_len=0;
	int ret;

    //printf("sizeof(TRANSMIT_DATA)=%d\n", (int)(sizeof(TRANSMIT_DATA)));

    while(sent_len < sizeof(TRANSMIT_DATA)){
        ret=write(client_socket, sent_buff+sent_len, sizeof(TRANSMIT_DATA));
        sent_len+=ret;
    }

    //printf("Socket: %s finished!\n", pcd.sent_ip);

    close(client_socket);

    free(sent_buff);
    free(td);

}



void parix_read_trace(char *trace_name){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        //printf("open file failed\n");
        exit(0);
    }


    char operation[100];
    char op_type[10];
    char offset[20];
    char size[10];
    char divider='\t';


    int i,j;
    int k;
    int access_start_chunk, access_end_chunk;
    int access_chunk_num;

    int ret;
    int num_rcrd_strp;
    int access_start_stripe, access_end_stripe;
    int count;
    int temp_start_chunk, temp_end_chunk;
	int its_prty_node_id;
	int index;

	int global_chunk_id;
	int node_id;


    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
    count=0;

    num_rcrd_strp=0;

    memset(mark_updt_stripes_tab, -1, sizeof(int)*max_updt_strps*(data_chunks+1));

	TRANSMIT_DATA* pcd_prty=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
	TRANSMIT_DATA* cmt_ntf_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
	TRANSMIT_DATA* updt_ack_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
	TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));


	char* recv_buff=(char *)malloc(sizeof(TRANSMIT_DATA));

	pthread_t send_cmd_thread[data_chunks+1];

	while(fgets(operation, sizeof(operation), fp)){

        count++;

        //printf("count=%d\n",count);

        new_strtok(operation,divider, op_type);
        new_strtok(operation,divider, offset);
        new_strtok(operation,divider, size);

        if((ret=strcmp(op_type, "Read"))==0)
            continue;


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

        //printf("access_start_stripe=%d, access_end_stripe=%d\n", access_start_stripe, access_end_stripe);
        //printf("access_start_chunk=%d, access_end_chunk=%d\n", access_start_chunk, access_end_chunk);

        //maintain a table to record the updates
        if(access_start_stripe==access_end_stripe){

            for(j=0; j<num_rcrd_strp; j++){
                if(mark_updt_stripes_tab[j*(data_chunks+1)]==access_start_stripe)
                    break;
            }

            if(j>=num_rcrd_strp){
                mark_updt_stripes_tab[j*(data_chunks+1)]=access_start_stripe;
                num_rcrd_strp++;
            }

            for(i=access_start_chunk; i<=access_end_chunk; i++){
                mark_updt_stripes_tab[j*(data_chunks+1)+i%data_chunks+1]++;
            }
        }

        else if(access_start_stripe<access_end_stripe){

            for(k=access_start_stripe; k<=access_end_stripe; k++){

                if(k==access_start_stripe){
                    temp_start_chunk=access_start_chunk;
                    temp_end_chunk=data_chunks-1;
                }

                else if(k==access_end_stripe){
                    temp_start_chunk=0;
                    temp_end_chunk=access_end_chunk%data_chunks;
                }

                else {
                    temp_start_chunk=0;
                    temp_end_chunk=data_chunks-1;
                }

                //check if stripe k is record
                for(j=0; j<num_rcrd_strp; j++){
                    if(mark_updt_stripes_tab[j*(data_chunks+1)]==k)
                        break;
                }

                if(j>=num_rcrd_strp){
                    mark_updt_stripes_tab[j*(data_chunks+1)]=k;
                    num_rcrd_strp++;
                }

                //record the updated data chunks in the k-th stripe
                for(i=temp_start_chunk; i<=temp_end_chunk; i++)
                    mark_updt_stripes_tab[j*(data_chunks+1)+i+1]++;

            }
        }

		//for each new write, send the data to its desined chunk 
		for(i=access_start_chunk; i<=access_end_chunk; i++){

			td->op_type=PARIX_UPDT;
			td->data_chunk_id=i;
			td->stripe_id=i/data_chunks;
			td->num_recv_chks_itn=-1;
			td->num_recv_chks_prt=-1;
		    td->port_num=UPDT_PORT;
			td->role=-1;
			td->updt_prty_id=-1;

			global_chunk_id=td->stripe_id*num_chunks_in_stripe+i%data_chunks;
		    node_id=global_chunk_map[global_chunk_id];
			td->chunk_store_index=locate_store_index(node_id, global_chunk_id);

			//init its associated parity node id
			for(j=0; j<num_chunks_in_stripe-data_chunks; j++)
				td->updt_prty_nd_id[j]=global_chunk_map[td->stripe_id*num_chunks_in_stripe+data_chunks+j];
			
			gene_radm_buff(td->buff, chunk_size);
			memcpy(td->sent_ip, node_ip_set[node_id], ip_len);

			//printf("send_ip=%s\n", td->sent_ip);
			
			send_data(td, td->sent_ip, td->port_num);

			//receive the ack from the updated data chunk 
			listen_ack(updt_ack_td, recv_buff, td->stripe_id, td->data_chunk_id, -1, UPDT_PORT, PARIX_UPDT_CMLT);

			}
		}

	/*  when finishing the trace, then commit the updates by sending the 
	    original data to the m parity nodes. */
	    
	//notify each old data to send its copy to all m parity chunks                              
	//for each stripe, find the updated data and send them the cmd 

    //printf("++++++++COMMIT STARTS: mark_updt_stripes_tab:\n");
	//print_array(num_rcrd_strp , data_chunks+1 , mark_updt_stripes_tab);
	int num_updt_dt_chnks=0;

	//in parix, when there is a stripe updated, we send a cmd to its parity nodes for parity updates.
	//for each chunk, the parity node will read its oldest version and newest version, and iteratively update the parity 
	for(k=0; k<num_chunks_in_stripe-data_chunks; k++){

		for(i=0; i<num_rcrd_strp; i++){

			for(j=0; j<data_chunks; j++){

			   if(mark_updt_stripes_tab[i*(data_chunks+1)+j+1]==-1)
			   	continue;

			   num_updt_dt_chnks=0;

			   //notify the parity node to be ready for receving old data first
			   pcd_prty->op_type=PARIX_CMMT;
			   pcd_prty->stripe_id=mark_updt_stripes_tab[i*(data_chunks+1)];
			   pcd_prty->updt_prty_id=k;
		       pcd_prty->num_recv_chks_prt=num_updt_dt_chnks;
		       pcd_prty->port_num=PARIX_UPDT_PORT+k;
		       pcd_prty->role=PARITY;	
			   pcd_prty->data_chunk_id=pcd_prty->stripe_id*data_chunks+j;

			   global_chunk_id=pcd_prty->stripe_id*num_chunks_in_stripe+data_chunks+k; 
			   node_id=global_chunk_map[global_chunk_id];
			   pcd_prty->chunk_store_index=locate_store_index(node_id, global_chunk_id);
			 
			   memset(pcd_prty->next_ip, '0', ip_len);
			   strcpy(pcd_prty->sent_ip, node_ip_set[node_id]);

			 
			   //printf("stripe=%d, prty_chunk=%d, pcd_prty->num_recv_chks_prt=%d\n", pcd_prty->stripe_id, k, pcd_prty->num_recv_chks_prt);
			   
			   //printf("PARITY! %s\n", pcd_prty->sent_ip);
			   pthread_create(&send_cmd_thread[data_chunks], NULL, parix_send_cmd_process, pcd_prty);
			   pthread_join(send_cmd_thread[data_chunks], NULL);
			   //printf("create_threads for parity succeeds:\n");
			
			   //listen the ack 
			   //collect the ack from the parity chunk before updating a next parity chunk
			   listen_ack(cmt_ntf_td, recv_buff, pcd_prty->stripe_id, -1, k, PARIX_UPDT_PORT+k, CMMT_CMLT);
			   //printf("Recv Ack Completes\n");
			}

			}
		}


	free(pcd_prty);
	free(recv_buff);
	free(cmt_ntf_td);
	free(td);
	free(updt_ack_td);
	
}



int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

    read_chunk_map("chunk_map");
	get_chunk_store_order();
	parix_read_trace(argv[1]);

    return 0;
}




