// this function is to generate the chunk distribution and keep it 
// the information include: 
//                         a. the mapping information of chunk_id to node_id

#define _GNU_SOURCE 


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <time.h>
#include <malloc.h>

#define total_nodes_num 9
#define rack_num 3
#define num_chunks_in_stripe 9
#define data_chunks 6
#define stripe_num 100000
#define max_chunks_per_rack 3
#define node_per_rack total_nodes_num/rack_num


//this function is to get the chunk distribution of each chunk 
//we assume that each rack is composed of a constant number of nodes 
void init_gen_chunk_distrbtn(){

   int i,j;
   int base;
   int rank;
   int prty_rack_num;
   int rack_id;
   int k;
   int batch;
   int temp_prty_rack_id;
   int index;
   int avail_rack;
   int start_node_id, end_node_id;
   int slct_rack;

   int* mark_if_slct=(int*)malloc(sizeof(int)*total_nodes_num);
   int* avail_rack_id=(int*)malloc(sizeof(int)*rack_num);
   int* chunk_to_node=(int *)malloc(sizeof(int)*num_chunks_in_stripe);
   int* chunk_map=(int*)malloc(sizeof(int)*stripe_num*num_chunks_in_stripe); // maps chunk_id to node_id
   int* chunk_num_in_racks=(int*)malloc(sizeof(int)*rack_num);
   int* prty_rack_id=(int*)malloc(sizeof(int)*rack_num);
   int* valid_place=(int*)malloc(sizeof(int)*(total_nodes_num+data_chunks-num_chunks_in_stripe));
   int* flag_index=(int*)malloc(sizeof(int)*(total_nodes_num+data_chunks-num_chunks_in_stripe));
   
   srand((unsigned int)time(0));

   prty_rack_num=ceil((num_chunks_in_stripe-data_chunks)/max_chunks_per_rack);
   printf("prty_rack_num=%d\n", prty_rack_num);

   for(i=0; i<stripe_num; i++){

	memset(mark_if_slct, 0, sizeof(int)*total_nodes_num);
	memset(chunk_to_node, -1, sizeof(int)*num_chunks_in_stripe);
	memset(chunk_num_in_racks, 0, sizeof(int)*rack_num);

	//random select the prty_rack_num to place parity chunks 
	for(j=0; j<rack_num; j++)
		prty_rack_id[j]=j;

	base=rack_num;

	for(j=0; j<prty_rack_num; j++){

		rank=rand()%base;
		temp_prty_rack_id=prty_rack_id[rank];

		if(j<prty_rack_num-1)
			batch=max_chunks_per_rack;
		else 
			batch=(num_chunks_in_stripe-data_chunks)-(prty_rack_num-1)*max_chunks_per_rack;

		for(k=0; k<batch; k++){
			chunk_to_node[data_chunks+j*max_chunks_per_rack+k]=temp_prty_rack_id*node_per_rack+k;	
			chunk_num_in_racks[temp_prty_rack_id]++;
			mark_if_slct[temp_prty_rack_id*node_per_rack+k]=1;
			}

		prty_rack_id[rank]=prty_rack_id[rack_num-rank-1];
		base--;
		
		}

	//select a rack for the data node
	for(j=0; j<data_chunks; j++){

		memset(avail_rack_id, -1, sizeof(int)*rack_num);

        //select a available rack to place the data chunk
		avail_rack=0;

		for(k=0; k<rack_num; k++)
			if(chunk_num_in_racks[k]<max_chunks_per_rack){
				avail_rack_id[avail_rack]=k;
				avail_rack++;
				}

		slct_rack=avail_rack_id[rand()%avail_rack];

		//select a node in the rack 
		start_node_id=node_per_rack*slct_rack;
		end_node_id=node_per_rack*(slct_rack+1)-1;

		//printf("slct_rack=%d, start_node_id=%d, end_node_id=%d\n", slct_rack, start_node_id, end_node_id);

		for(k=start_node_id; k<=end_node_id; k++){

			if(mark_if_slct[k]==1)
				continue;

			chunk_to_node[j]=k;
			chunk_num_in_racks[slct_rack]++;
			mark_if_slct[k]=1;

			break;

			}

		}

	//printf("%d-th stripe node_map:\n",i);

	for(j=0; j<num_chunks_in_stripe; j++)
		chunk_map[i*num_chunks_in_stripe+j]=chunk_to_node[j];

   	}

   //print the chunk_map
   //printf("chunk_map:\n");
   //print_array(stripe_num, num_chunks_in_stripe, chunk_map);


   //write the mapping info to a chunk_2_node 
   FILE *fd; 

   fd=fopen("chunk_map","w");
   for(i=0; i<stripe_num; i++){

	for(j=0; j<num_chunks_in_stripe; j++)
		fprintf(fd, "%d ", chunk_map[i*num_chunks_in_stripe+j]);

	fprintf(fd, "\n");
   	}

   fclose(fd);


   free(chunk_map);
   free(chunk_to_node);
   free(flag_index);
   free(chunk_num_in_racks);
   free(prty_rack_id);
   free(valid_place);


}


int main(int argc, char** argv){

  init_gen_chunk_distrbtn();

  //printf("read_chunk_map:\n");
  //print_array(stripe_num, num_chunks_in_stripe, global_chunk_map);

  return 0;

}

