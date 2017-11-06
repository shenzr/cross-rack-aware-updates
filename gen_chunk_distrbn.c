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

#include "config.h"
#include "common.h"


//this function is to get the chunk distribution of each chunk 
//we assume that each rack is composed of a constant number of nodes 
void init_gen_chunk_distrbtn(){

   int i,j;
   int base;
   int rank;
   int starting_rack;

   int temp_rack_id;
   int temp_node_id;
   int starting_node_count;
   int k;


   int max_nodes_racks;

   int* flag_index=malloc(sizeof(int)*data_chunks);
   int* chunk_order=malloc(sizeof(int)*num_chunks_in_stripe);
   int* chunk_map=malloc(sizeof(int)*stripe_num*num_chunks_in_stripe); // maps chunk_id to node_id
   int* base_array=malloc(sizeof(int)*rack_num);
   int* count=malloc(sizeof(int)*rack_num);

   max_nodes_racks=find_max_array(nodes_in_racks,  rack_num);

   int* flag_array=malloc(sizeof(int)*rack_num*max_nodes_racks);
   
   srand((unsigned int)time(0));

   for(i=0; i<stripe_num; i++){

	memset(chunk_order, -1, sizeof(int)*num_chunks_in_stripe);

	starting_rack = i%rack_num;
	
	//generate the random order of the k data chunks 
	base = data_chunks;

	for(j=0;j<data_chunks;j++)
	   flag_index[j]=j;

    // generate placement order of data chunks
	for(j=0;j<data_chunks;j++){

	   rank=rand()%base;
		
	   //record the mapping from chunks to nodes 
	   chunk_order[j]=flag_index[rank];
	   flag_index[rank]=flag_index[data_chunks-j-1];
	   base--;

		}

	// place the data chunks starting at the starting rack
    for(j=data_chunks; j<num_chunks_in_stripe; j++)
		chunk_order[j]=j;

	for(j=0; j<rack_num; j++)
		base_array[j]=nodes_in_racks[j];

	for(j=0; j<rack_num; j++){

		for(k=0; k<nodes_in_racks[j]; k++)
			flag_array[j*max_nodes_racks+k]=k;
			
		}

	//printf("the order is\n");
	//for(j=0; j<num_chunks_in_stripe; j++)
		//printf("%d ", chunk_order[j]);
	//printf("\n");

	memset(count, 0, sizeof(int)*rack_num);

	for(j=0; j<num_chunks_in_stripe; j++){

		//if it is a parity chunk 
		if(j>=data_chunks)
			temp_rack_id=(starting_rack+j/max_chunks_per_rack)%rack_num;

        //determine the rack_id and randomly choose a node
        else 
			temp_rack_id=(starting_rack+chunk_order[j]/max_chunks_per_rack)%rack_num;

		starting_node_count=0;
		for(k=0; k<temp_rack_id; k++)
			starting_node_count+=nodes_in_racks[k];

		rank=rand()%base_array[temp_rack_id];
		temp_node_id=flag_array[temp_rack_id*max_nodes_racks+rank]+starting_node_count;
		flag_array[temp_rack_id*max_nodes_racks+rank]=flag_array[temp_rack_id*max_nodes_racks+base_array[temp_rack_id]-count[temp_rack_id]-1];
		
		base_array[temp_rack_id]--;
		count[temp_rack_id]++;

		//printf("chunk %d, node_id=%d, \n", j, temp_node_id, starting_node_count);

        //printf("%d-th chunk, temp_rack_id=%d, rank=%d, temp_node_id=%d\n",j, temp_rack_id, rank, temp_node_id);
		//print_array(rack_num, max_nodes_racks, flag_array);

		chunk_map[i*num_chunks_in_stripe+j]=temp_node_id;
		
		}
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


   free(flag_index);
   free(chunk_map);
   free(chunk_order);
   free(count);
   free(base_array);
   free(flag_array);


}


int main(int argc, char** argv){

  init_gen_chunk_distrbtn();
  read_chunk_map("chunk_map");

  //printf("read_chunk_map:\n");
  //print_array(stripe_num, num_chunks_in_stripe, global_chunk_map);

  return 0;

}

