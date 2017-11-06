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

#define PORT_NUM 1111
#define UPPBND   9999

int mark_updt_stripes_tab[max_updt_strps*(data_chunks+1)]; //it records the updated data chunks and their stripes, the data_chunk column stores the data, while the data_chunk-th column store the stripe_id
int prty_log_map[stripe_num*data_chunks];
int sorted_updt_strp_tab[max_updt_strps*(data_chunks+1)]; //the sorted data chunks that are accessed in each accessed stripe, it is used for data separation


// sort the data access frequencies in descending order
void quick_sort(int* data, int* index, int start_id, int end_id){

	int left=start_id;
	int right=end_id;
	int mid; 
	int p=start_id; //the point

	int guard=data[start_id];
	int guard_id=index[start_id];

	while(left<right){

		while(data[right]<=guard && right>p)
			right--;

		if(data[right]>guard){

			data[p]=data[right];
			index[p]=index[right];
			p=right;
			
			}

		while(data[left]>=guard && left<p)
			left++;

		if(data[left]<guard){

			data[p]=data[left];
			index[p]=index[left];
			p=left;
			
			}
		}

	data[p]=guard;
	index[p]=guard_id;

	if(p-start_id>1)
		quick_sort(data,index,start_id,p-1);

	if(end_id-p>1)
		quick_sort(data,index,p+1,end_id);

}


// send the update command to a node
// port_num=6666
void* send_cmd_process(void* ptr){

    THREAD_CMD_DATA tcd = *(THREAD_CMD_DATA *)ptr;
    //printf("---->send_cmd_process: to %s", tcd.sent_ip);
    TRANSMIT_DATA *td = malloc(sizeof(TRANSMIT_DATA));

    td->op_type=tcd.op_type;
    td->stripe_id=tcd.stripe_id;
    td->data_chunk_id=tcd.data_chunk_id;
    td->updt_prty_id=tcd.updt_prty_id;
    td->num_recv_chks_itn=tcd.num_recv_chks_itn;
    td->num_recv_chks_prt=tcd.num_recv_chks_prt;
    td->role=tcd.role;
	td->port_num=tcd.port_num;
	td->chunk_store_index=tcd.chunk_store_index;
	
	memcpy(td->next_ip, tcd.next_ip, 50);
	
    //printf("tcd_init_succeeds, port_num=%d\n", tcd.port_num);

    char server_ip[50];
    memcpy(server_ip, tcd.sent_ip, 50);
    int port_num=PORT_NUM;

    int ret;
    int on;
    int sent_len;

    //set client_addr info
    struct sockaddr_in client_addr;
    bzero(&client_addr,sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_addr.s_addr = htons(INADDR_ANY);
    client_addr.sin_port = htons(0);

    //create client socket
    int client_socket = socket(AF_INET,SOCK_STREAM,0);
    on=1;
    ret = setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on) );

    if( client_socket < 0)
    {
        printf("Create Socket Failed!\n");
        exit(1);
    }

    //combine client_socket with client_addr
    if(bind(client_socket,(struct sockaddr*)&client_addr,sizeof(client_addr)))
    {
        printf("Client Bind Port Failed!\n");
        exit(1);
    }

    //printf("client_ip_addr=%s\n", inet_ntoa(client_addr.sin_addr));

    //set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    //printf("connected_server_ip=%s\n", server_ip);

    if(inet_aton(server_ip,&server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
        exit(1);
    }
    server_addr.sin_port = htons(port_num);
    socklen_t server_addr_length = sizeof(server_addr);

    //connect server
    while(connect(client_socket,(struct sockaddr*)&server_addr, server_addr_length) < 0);

    //printf("connect success!\n");

    //init sent_buff
    char *sent_buff=malloc(sizeof(TRANSMIT_DATA));

    memcpy(sent_buff, td, sizeof(TRANSMIT_DATA));

    sent_len=0;

    //printf("sizeof(TRANSMIT_DATA)=%d\n", (int)(sizeof(TRANSMIT_DATA)));

    while(sent_len < sizeof(TRANSMIT_DATA)){
        ret=write(client_socket, sent_buff+sent_len, sizeof(TRANSMIT_DATA));
        sent_len+=ret;
    }

    //printf("Socket: %s finished!\n", server_ip);

    close(client_socket);

    free(sent_buff);
    free(td);

}

//@in_chunk_id:  the hot chunk to be move in the hot rack
//@out_chunk_id: the cold chunk to be move from the hot rack
void pfrm_chnk_movmt(int in_chunk_id, int in_chnk_node_id, int out_chunk_id, int out_chnk_node_id){

	int i;
	int temp;
	int in_store_order, out_store_order;
	
	//send cmd data for separation to the related two nodes 
	TRANSMIT_DATA* mvmn_cmd_mt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*2);

    //for in-chunk 
	mvmn_cmd_mt[0].op_type=CMD_MVMNT;
	mvmn_cmd_mt[0].stripe_id=in_chunk_id/num_chunks_in_stripe;
	mvmn_cmd_mt[0].data_chunk_id=mvmn_cmd_mt[0].stripe_id*data_chunks+in_chunk_id%num_chunks_in_stripe;
	mvmn_cmd_mt[0].updt_prty_id=-1;
	mvmn_cmd_mt[0].port_num=MVMT_PORT;
	mvmn_cmd_mt[0].role=IN_CHNK;
	mvmn_cmd_mt[0].chunk_store_index=locate_store_index(in_chnk_node_id, in_chunk_id);
	memcpy(mvmn_cmd_mt[0].sent_ip, node_ip_set[in_chnk_node_id], ip_len);

    //for out-chunk
	mvmn_cmd_mt[1].op_type=CMD_MVMNT;
	mvmn_cmd_mt[1].stripe_id=out_chunk_id/num_chunks_in_stripe;
	mvmn_cmd_mt[1].data_chunk_id=mvmn_cmd_mt[0].stripe_id*data_chunks+out_chunk_id%num_chunks_in_stripe;
	mvmn_cmd_mt[1].updt_prty_id=-1;
	mvmn_cmd_mt[1].port_num=MVMT_PORT;
	mvmn_cmd_mt[1].role=OUT_CHNK;
	mvmn_cmd_mt[1].chunk_store_index=locate_store_index(out_chnk_node_id, out_chunk_id);
	memcpy(mvmn_cmd_mt[1].sent_ip, node_ip_set[out_chnk_node_id], ip_len);

	//send the movement cmd
	pthread_t send_cmd_thread[2];
	memset(send_cmd_thread, 0, sizeof(send_cmd_thread));
	
	for(i=0; i<2; i++)
		pthread_create(&send_cmd_thread[i], NULL, send_cmd_process, mvmn_cmd_mt+i);

	for(i=0; i<2; i++)
		pthread_join(send_cmd_thread[i], NULL);

	//receive data from the two nodes and perform movement
	para_recv_data(mvmn_cmd_mt[0].data_chunk_id, 2, MVMT_PORT, 2);

	//send back the data to the two nodes
	//update the two structures 
	mvmn_cmd_mt[0].op_type=DATA_MVMNT;
	memcpy(mvmn_cmd_mt[0].sent_ip, node_ip_set[out_chnk_node_id], ip_len); //send the hot data to the node which stores the cold chunk in the hot rack
	memcpy(mvmn_cmd_mt[0].buff, in_chunk, chunk_size);

	mvmn_cmd_mt[1].op_type=DATA_MVMNT;
	memcpy(mvmn_cmd_mt[1].sent_ip, node_ip_set[in_chnk_node_id], ip_len);
	memcpy(mvmn_cmd_mt[1].buff, out_chunk, chunk_size);

	for(i=0; i<2; i++)
		pthread_create(&send_cmd_thread[i], NULL, send_cmd_process, mvmn_cmd_mt+i);	
	
	for(i=0; i<2; i++)
		pthread_join(send_cmd_thread[i], NULL);

	//update chunk_map and chunk_store_order
	temp=global_chunk_map[in_chunk_id];
	global_chunk_map[in_chunk_id]=global_chunk_map[out_chunk_id];
	global_chunk_map[out_chunk_id]=temp;

	in_store_order=locate_store_index(in_chnk_node_id, in_chunk_id);
	chunk_store_order[in_chnk_node_id*max_num_store_chunks+in_store_order]=out_chunk_id;

	out_store_order=locate_store_index(out_chnk_node_id, out_chunk_id);
	chunk_store_order[out_chnk_node_id*max_num_store_chunks+out_store_order]=in_chunk_id;

	//printf("in_store_order=%d\n, out_store_order=%d\n", in_store_order, out_store_order);
	
	//printf("movement for two chunks finishes!\n");

	free(mvmn_cmd_mt);
	
}


//this function is to sort the data chunks in a stripe, based on their access frequencies
//we assume that the data are separated for each commit 
void hot_cold_separation(int num_rcrd_strp){

  //printf("Hot_cold_separation starts:\n");

  int i;
  int j;
  int temp_rack_id;
  int slct_rack;
  int index;
  int start_node_id, end_node_id;
  int k,l;
  int search_start;

  int in_chunk_id, in_chnk_nd_id;
  int out_chunk_id, out_chnk_nd_id;

  int temp_stripe[data_chunks];
  int temp_index[data_chunks];

  int rcd_rack_id[rack_num];

  //sort each stripe 
  for(i=0; i<num_rcrd_strp; i++){

	//printf("separation for stripe_id=%d\n", mark_updt_stripes_tab[i*(data_chunks+1)]);

	for(j=0; j<data_chunks; j++)
		temp_index[j]=j;

	for(j=0; j<data_chunks; j++)
		temp_stripe[j]=mark_updt_stripes_tab[i*(data_chunks+1)+j+1];

    //printf("map_node:\n");
	//for(j=0; j<data_chunks; j++)
		//printf("%d ", global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+j]);
	//printf("\n");
	
	//sort the data chunks with their indices
	quick_sort(temp_stripe, temp_index, 0, data_chunks-1);

	memset(rcd_rack_id, 0, sizeof(int)*rack_num);

	//find where the first t data chunks resides
	for(j=0; j<max_chunks_per_rack; j++){

        //we only consider the chunks that are accessed
		if(temp_stripe[j]==-1)
			break;

		temp_rack_id=get_rack_id(global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+temp_index[j]]);
		rcd_rack_id[temp_rack_id]++;

		}

	//printf("num_hot_chnk_per_rack:\n");
	//print_array(1, rack_num, rcd_rack_id);

	//locate the destine rack id that has the maximum number of t hottest chunks
	slct_rack=find_max_array_index(rcd_rack_id, rack_num);
	//printf("slct_rack=%d\n", slct_rack);

    search_start=max_chunks_per_rack;
	//perform separation
	for(j=0; j<max_chunks_per_rack; j++){

        //if the following chunks are not accessed  
		if(temp_stripe[j]==-1)
			break;

		temp_rack_id=get_rack_id(global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+temp_index[j]]);

		if(temp_rack_id!=slct_rack){

			//printf("the hot chunk to be moved: orig_index=%d, sorted_index=%d\n", temp_index[j], j);

			//otherwise, select a cold chunk from this rack and perform switch
			for(k=search_start; k<data_chunks; k++){

				//printf("temp_index[k]=%d\n",temp_index[k]);

				temp_rack_id=get_rack_id(global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+temp_index[k]]);

				if(temp_rack_id==slct_rack){

					in_chunk_id=mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+temp_index[j];
					in_chnk_nd_id=global_chunk_map[in_chunk_id];
					out_chunk_id=mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+temp_index[k];
					out_chnk_nd_id=global_chunk_map[out_chunk_id];

					//printf("selected_out_chunk=%d, selected_out_node=%d\n", out_chunk_id, out_chnk_nd_id);
					pfrm_chnk_movmt(in_chunk_id, in_chnk_nd_id, out_chunk_id, out_chnk_nd_id);

					search_start=k+1;//set the value for next search
					
					}
				}
			}
		}
	}

  //printf("data_separation completes\n");
}


void cau_update(int chunk_id){

   int node_id;
   int log_prty_id;
   int global_chunk_id;

   TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
   TRANSMIT_DATA* ntf_dt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
   char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

   td->op_type=DATA_UPDT;
   td->data_chunk_id=chunk_id;
   td->stripe_id=chunk_id/data_chunks;
   td->port_num=PORT_NUM;

   node_id=global_chunk_map[td->stripe_id*num_chunks_in_stripe+chunk_id%data_chunks];

   //tell the data chunk where its corresponding parity chunk is 
   log_prty_id=prty_log_map[chunk_id];
   memcpy(td->next_ip, node_ip_set[global_chunk_map[td->stripe_id*num_chunks_in_stripe+log_prty_id]], ip_len);

   //printf("stripe_id=%d, chunk_id=%d, log_prty_id=%d, log_prty_ip=%s\n", td->stripe_id, chunk_id%data_chunks, log_prty_id, td->next_ip);

   //randomly generate the updated data
   gene_radm_buff(td->buff,chunk_size);

   //printf("gene_radm_buff succeeds\n");
   //printf("stripe_id=%d, chunk_id=%d, node_id=%d\n", chunk_id/data_chunks, chunk_id, node_id);

   //find the server
   //printf("data_chunk_id=%d\n", td->data_chunk_id);
   //printf("server_ip=%s\n", node_ip_set[node_id]);

   //log write the updated data
   send_updt_data(td, node_ip_set[node_id], td->port_num);

   //listen ack from data chunk 
   listen_ack(ntf_dt, recv_buff, chunk_id/data_chunks, chunk_id, -1, UPDT_ACK_PORT, LOG_CMLT);

   free(ntf_dt);
   free(td);
   free(recv_buff);

}


//establish the map of a data chunk to a parity chunk. This parity chunk is used for log a replica of the data chunk, so as to 
//promise one chunk/rack failure tolerance
void cau_estbh_log_map(){

	//printf("enter cau_estbh_log_map:\n");

	int i,j; 
	int k,h;
	int r;
	int its_node_id;
	int its_rack_id;
	int dt_rack_id;
	int prty_rack_id;
	int flag;

	int min_load;
	int slct_prty_id;

	int map_load[num_chunks_in_stripe-data_chunks]; //it records the number of logs to each parity chunk

	memset(prty_log_map, -1, sizeof(int)*stripe_num*data_chunks);

	for(i=0; i<stripe_num; i++){

		//printf("i=%d\n", i);
		
		memset(map_load, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));
		
		//start from the rack with parity chunks 
		for(r=0; r<rack_num; r++){

            //locate the rack that has parity chunks
			for(j=data_chunks; j<num_chunks_in_stripe; j++){

				//locate its node and rack
				its_node_id=global_chunk_map[i*num_chunks_in_stripe+j];
				its_rack_id=get_rack_id(its_node_id);

				if(its_rack_id==r)
					break;

				}

			//if there is no parity chunk in this rack, then continue
			if(j==num_chunks_in_stripe)
				continue;

			//printf("%d-th rack has parity chunks\n", r);

			//find the data chunks in this rack 
			for(k=0; k<data_chunks; k++){

				dt_rack_id=get_rack_id(global_chunk_map[i*num_chunks_in_stripe+k]);

				if(dt_rack_id==r){

					min_load=UPPBND;
					flag=0;
					
					//find the approate parity in other racks
					for(h=data_chunks; h<num_chunks_in_stripe; h++){

						prty_rack_id=get_rack_id(global_chunk_map[i*num_chunks_in_stripe+h]);

						//printf("h=%d, h_rack_id=%d, r=%d\n", h, prty_rack_id, r);

						if(prty_rack_id!=r){

							if(map_load[h-data_chunks]<min_load){
								
								slct_prty_id=h;
								min_load=map_load[h-data_chunks];
								flag=1;

								//printf("slct_prty_id=%d, min_load=%d\n", slct_prty_id, min_load);

								}
							}
						}

					//update the map_load
					prty_log_map[i*data_chunks+k]=slct_prty_id;
					map_load[slct_prty_id-data_chunks]++;

					}
				}
			}

		for(j=0; j<data_chunks; j++){

			if(prty_log_map[i*data_chunks+j]!=-1)
				continue;

            min_load=UPPBND;
			//just need to scan the parity chunks and see their map_load 
			for(k=data_chunks; k<num_chunks_in_stripe; k++){

				if(map_load[k-data_chunks]<min_load){

					min_load=map_load[k-data_chunks];
					slct_prty_id=k;

					}
				}

			//update the map_load
			prty_log_map[i*data_chunks+j]=slct_prty_id;
			map_load[slct_prty_id-data_chunks]++;

			}

		}

	//write the mapping info into a file 
	FILE *fd; 
	char* filename="parity_log_map";
	
	fd=fopen(filename,"w");
	if(fd==NULL)
		printf("openfile error!\n");
	
	for(i=0; i<stripe_num; i++){
	
	 for(j=0; j<data_chunks; j++)
		 fprintf(fd, "%d ", prty_log_map[i*data_chunks+j]);
	
	 fprintf(fd, "\n");
	 }
	
	fclose(fd);

	printf("finish parity_log_map\n");

	
}


void cau_commit_data(int num_rcrd_strp){

   //printf("----->parity commit starts:\n"); 


   int index;
   int i,j,k; 
   int tmp_rack_id;
   int tmp_node_id;
   int its_node_id;
   int its_next_node_id;
   int prty_node_id;
   int global_chunk_id;

   THREAD_CMD_DATA* tcd=(THREAD_CMD_DATA*)malloc(sizeof(THREAD_CMD_DATA));
   TRANSMIT_DATA* cmt_ntf_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
   THREAD_CMD_DATA* tcd_mt = (THREAD_CMD_DATA*)malloc(sizeof(THREAD_CMD_DATA) * data_chunks);

   int* flag_itnl=(int*)malloc(sizeof(int)*rack_num); 
   int* intnl_nds=(int*)malloc(sizeof(int)*rack_num); //record the internal nodes in each rack
   char* recv_buff=(char *)malloc(sizeof(TRANSMIT_DATA));

   pthread_t send_cmd_thread[total_nodes_num];

   //commit the updated data in each stripe
   for(k=0; k<num_chunks_in_stripe-data_chunks; k++){

       //commit the data
       //printf("commit data starts:\n");

       for(i=0; i<num_rcrd_strp; i++){

	      //printf("Commit At Stripe-%d\n", mark_updt_stripes_tab[i*(data_chunks+1)]);

		  memset(send_cmd_thread, 0, sizeof(send_cmd_thread));
				  
	      //send the request to the updated parity chunks
		  tcd->op_type=DATA_COMMIT;
		  tcd->num_recv_chks_itn=0;
		  tcd->data_chunk_id=-1;
		  tcd->updt_prty_id=k;
		  tcd->role=PARITY;
		  memset(tcd->next_ip, '0', 50);

          //for different nodes, send different data
          //we set the first data chunk in a rack as the internal node
          memset(flag_itnl, 0, sizeof(int)*rack_num);
          memset(intnl_nds, -1, sizeof(int)*rack_num);

         //find the internal nodes
         for(j=0; j<data_chunks; j++){

            //if it is a stripe for commit
            if(mark_updt_stripes_tab[i*(data_chunks+1)+j+1]>=0){

                tmp_node_id=global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+j];
                tmp_rack_id=get_rack_id(tmp_node_id);

                //printf("temp_node_id=%d\n", tmp_node_id);
                //printf("temp_rack_id=%d\n", tmp_rack_id);

                //it is the internal node
                if(flag_itnl[tmp_rack_id]==0)
                     intnl_nds[tmp_rack_id]=tmp_node_id;

                flag_itnl[tmp_rack_id]++;

               }
           }

		  tcd->stripe_id=mark_updt_stripes_tab[i*(data_chunks+1)];
		  tcd->port_num=UPDT_PORT+i; //increase the port_num stripe by stripe

		  global_chunk_id=tcd->stripe_id*num_chunks_in_stripe+data_chunks+k;
		  strcpy(tcd->sent_ip, node_ip_set[global_chunk_map[global_chunk_id]]);
	      tcd->num_recv_chks_prt=count_non_ngtv(intnl_nds,rack_num);

		  prty_node_id=global_chunk_map[global_chunk_id];
		  tcd->chunk_store_index=locate_store_index(prty_node_id, global_chunk_id);
		  
		  //printf("\n");
		  //print_array(1, 3, flag_itnl);
		  //print_array(1, 3, intnl_nds);
		  //printf("mark_updt_stripes_tab:\n");
		  //print_array(num_rcrd_strp, data_chunks+1, mark_updt_stripes_tab);
				  

		  //printf("stripe=%d, prty_chunk=%d, tcd->num_recv_chks_prt=%d\n\n", tcd->stripe_id, k, tcd->num_recv_chks_prt);

		  //printf("PARITY! %s\n", tcd->sent_ip);
		  pthread_create(&send_cmd_thread[data_chunks+k], NULL, send_cmd_process, tcd);
		  pthread_join(send_cmd_thread[data_chunks+k], NULL);
		  //printf("create_threads for parity succeeds:\n");

          index=0;

          // find internal nodes first and notify them before leaf nodes
          for(j=0; j<data_chunks; j++){

              if(mark_updt_stripes_tab[i*(data_chunks+1)+j+1]>=0){

                  tmp_node_id=global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+j];
                  tmp_rack_id=get_rack_id(tmp_node_id);

                  if(intnl_nds[tmp_rack_id]==tmp_node_id){

                     tcd_mt[index].op_type=DATA_COMMIT;
                     tcd_mt[index].num_recv_chks_itn=flag_itnl[tmp_rack_id]-1; //should minus the updated chunk on the internal node
                     tcd_mt[index].role=INTERNAL;
                     //printf("INTERNAL!\n");

                     tcd_mt[index].stripe_id=mark_updt_stripes_tab[i*(data_chunks+1)];
                     strcpy(tcd_mt[index].next_ip, node_ip_set[global_chunk_map[tcd_mt[index].stripe_id*num_chunks_in_stripe+data_chunks+k]]); // the next ip of internal node is the parity node

				     //printf("Updt_Chnk=%d, It is the INTERNAL of stripe %d\n", k, tcd_mt[index].stripe_id);

					 global_chunk_id=tcd_mt[index].stripe_id*num_chunks_in_stripe+j;
                     its_node_id=global_chunk_map[global_chunk_id];
					 tcd_mt[index].chunk_store_index=locate_store_index(its_node_id, global_chunk_id);
					 
                     tcd_mt[index].data_chunk_id=tcd_mt[index].stripe_id*data_chunks+j;
                     tcd_mt[index].updt_prty_id=k;
                     tcd_mt[index].port_num=UPDT_PORT+i;
                     strcpy(tcd_mt[index].sent_ip, node_ip_set[its_node_id]);
                     tcd_mt[index].num_recv_chks_prt=-1;

					 //printf("INTERNAL: tcd_mt[index].data_chunk_id=%d,  tcd_mt[index].chunk_store_index=%d\n", tcd_mt[index].data_chunk_id,  tcd_mt[index].chunk_store_index);

				     //send the request to the data_chunks by using multiple threads
					 pthread_create(&send_cmd_thread[index], NULL, send_cmd_process, tcd_mt+index);
								
					 index++;

                   }
                 }

             }

           // find the leaf nodes and notify them for the data commit
	      for(j=0; j<data_chunks; j++){

			 if(mark_updt_stripes_tab[i*(data_chunks+1)+j+1]>=0){

			    tmp_node_id=global_chunk_map[mark_updt_stripes_tab[i*(data_chunks+1)]*num_chunks_in_stripe+j];
				tmp_rack_id=get_rack_id(tmp_node_id);

				if(intnl_nds[tmp_rack_id]!=tmp_node_id){

					tcd_mt[index].op_type=DATA_COMMIT;
					tcd_mt[index].role=LEAF;
					its_next_node_id=intnl_nds[tmp_rack_id];
					//printf("LEAF!\n");
					//printf("Its_internal_node_id=%d\n", its_next_node_id);
		
					strcpy(tcd_mt[index].next_ip, node_ip_set[its_next_node_id]);

					tcd_mt[index].stripe_id=mark_updt_stripes_tab[i*(data_chunks+1)];
                    tcd_mt[index].data_chunk_id=tcd_mt[index].stripe_id*data_chunks+j;
                    

					global_chunk_id=tcd_mt[index].stripe_id*num_chunks_in_stripe+j;
                    its_node_id=global_chunk_map[global_chunk_id];
					tcd_mt[index].chunk_store_index=locate_store_index(its_node_id, global_chunk_id);

					//printf("Updt_Chnk=%d, It is the LEAF of stripe %d\n", k, tcd_mt[index].stripe_id);
					//printf("LEAF: tcd_mt[index].data_chunk_id=%d,  tcd_mt[index].chunk_store_index=%d\n", tcd_mt[index].data_chunk_id,  tcd_mt[index].chunk_store_index);
							
                    tcd_mt[index].updt_prty_id=k;
                    tcd_mt[index].port_num=UPDT_PORT+i;
                    strcpy(tcd_mt[index].sent_ip, node_ip_set[its_node_id]);
                    tcd_mt[index].num_recv_chks_prt=-1;
				    tcd_mt[index].num_recv_chks_itn=-1;

					//send the request to the data_chunks by using multiple threads
					pthread_create(&send_cmd_thread[index], NULL, send_cmd_process, tcd_mt+index);
							
					index++;

				}
			}
	    }

         for(j=0; j<index; j++)
             pthread_join(send_cmd_thread[j], NULL); 

	     //collect the ack from the parity chunk before updating a next parity chunk
		 listen_ack(cmt_ntf_td, recv_buff, tcd->stripe_id, -1, k, UPDT_PORT+i, CMMT_CMLT);

		 //printf("Recv Ack Completes\n");
				   
              }
           }

   free(tcd);
   free(tcd_mt);
   free(cmt_ntf_td);
   free(flag_itnl);
   free(intnl_nds);
   free(recv_buff);

}




//read a trace
void cau_read_trace(char *trace_name){

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
    int total_num_req;
    int ret;
    int num_rcrd_strp;
    int access_start_stripe, access_end_stripe;
    int count;
    int temp_start_chunk, temp_end_chunk;


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

    memset(mark_updt_stripes_tab, -1, sizeof(int)*max_updt_strps*(data_chunks+1));
	
    while(fgets(operation, sizeof(operation), fp)) {

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

        //check if the accessed stripe has been recorded
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
                    mark_updt_stripes_tab[j*(data_chunks+1)+i%data_chunks+1]++;

            }
        }

        total_num_req++;

        //perform log_write for each updated chunk
        for(i=access_start_chunk; i<=access_end_chunk; i++){
			cau_update(i);
            //printf("log_write succeeds\n");

        }

        //check if the table is full. If it is, then invoke the commit task
        if(num_rcrd_strp==max_updt_strps){
			
			cau_commit_data(num_rcrd_strp);
			hot_cold_separation(num_rcrd_strp);

			//re-init the mark_updt_stripes_table
			memset(mark_updt_stripes_tab, -1, sizeof(int)*max_updt_strps*(data_chunks+1));
			num_rcrd_strp=0;
			
        	}
    }

	//when the trace is completely perform, commit the data deltas
	cau_commit_data(num_rcrd_strp);
	hot_cold_separation(num_rcrd_strp);

    fclose(fp);

}





int main(int argc, char** argv){

    if(argc!=2){
        printf("./client_size trace_name!\n");
        exit(0);
    }

    read_chunk_map("chunk_map");
	cau_estbh_log_map();
	get_chunk_store_order();
    cau_read_trace(argv[1]);

    return 0;
}

