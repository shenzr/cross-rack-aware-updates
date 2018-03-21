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
void init_parix_fo_gen_chunk_distrbtn(){

   int i,j;
   int base;
   int rank;

   int* chunk_to_node=malloc(sizeof(int)*num_chunks_in_stripe);
   int* parix_fo_chunk_map=malloc(sizeof(int)*stripe_num*num_chunks_in_stripe); // maps chunk_id to node_id
   int* flag_index=(int*)malloc(sizeof(int)*total_nodes_num);
   
   srand((unsigned int)time(0));

   for(i=0; i<stripe_num; i++){

	memset(chunk_to_node, -1, sizeof(int)*num_chunks_in_stripe);
	
	//generate the distribution of each stripe randomly

	for(j=0; j<total_nodes_num; j++)
		flag_index[j]=j;

	base=total_nodes_num;

	for(j=0; j<num_chunks_in_stripe; j++){

		rank=rand()%base;

		chunk_to_node[j]=flag_index[rank];
		flag_index[rank]=flag_index[total_nodes_num-j-1];

		base--;
		
		}

	//printf("%d-th stripe node_map:\n",i);
	for(j=0; j<num_chunks_in_stripe; j++)
		parix_fo_chunk_map[i*num_chunks_in_stripe+j]=chunk_to_node[j];

   	}

   //print the chunk_map
   //printf("chunk_map:\n");
   //print_array(stripe_num, num_chunks_in_stripe, chunk_map);


   //write the mapping info to a chunk_2_node 
   FILE *fd; 

   fd=fopen("parix_fo_chunk_map","w");
   for(i=0; i<stripe_num; i++){

	for(j=0; j<num_chunks_in_stripe; j++)
		fprintf(fd, "%d ", parix_fo_chunk_map[i*num_chunks_in_stripe+j]);

	fprintf(fd, "\n");
   	}

   fclose(fd);


   free(parix_fo_chunk_map);
   free(chunk_to_node);
   free(flag_index);


}


int main(int argc, char** argv){

  init_parix_fo_gen_chunk_distrbtn();
  read_chunk_map("parix_fo_chunk_map");

  //printf("read_chunk_map:\n");
  //print_array(stripe_num, num_chunks_in_stripe, global_chunk_map);

  return 0;

}

