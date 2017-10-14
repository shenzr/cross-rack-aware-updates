// this function is to generate the chunk distribution and keep it 
// the information include: 
//                         a. the mapping information of chunk_id to node_id

#define _GNU_SOURCE 


#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/types.h>
#include <time.h>
#include <malloc.h>


#include "config.h"

void print_array(int row, int col, int *a){

  int i,j; 

  for(i=0; i<row; i++){

	for(j=0; j<col; j++)
		printf("%d ", a[i*col+j]);

	printf("\n");
	
  	}

}


void gen_chunk_distrbtn(){

   int i,j;
   int base;
   int rank;

   int *flag_index=(int *)malloc(sizeof(int)*total_nodes_num);

   int *chunk_map=(int *)malloc(sizeof(int)*stripe_num*num_chunks_in_stripe); // maps chunk_id to node_id

   // map the chunks to the nodes randomly, and reach rack-level tolerance (the number of chunks in a rack should be less than m for every stripe)
   srand(time(0));
   
   for(i=0;i<stripe_num;i++){

	  base=total_nodes_num;
    
	  for(j=0;j<total_nodes_num;j++)
		 flag_index[j]=j;

      // generate num_chunks_in_stripe different numbers from the range [0,total_nodes_num]
	  for(j=0;j<num_chunks_in_stripe;j++){

	     rank=rand()%base;
		
		 // record the mapping from chunks to nodes 
		 chunk_map[i*num_chunks_in_stripe+j]=flag_index[rank];

		 flag_index[rank]=flag_index[total_nodes_num-j-1];

		 // mark this node has a chunk 
		 // the candidate nodes reduces by 1
		 //printf("rank=%d,k=%d\n",rank,k);

		 base--;

		}

  	}


   //print the chunk_map

   printf("chunk_map:\n");
   print_array(stripe_num, num_chunks_in_stripe, chunk_map);


   //write the mapping info to a chunk_2_node 
   FILE *fd; 

   fd=fopen("chunk_node_map","w");
   for(i=0; i<stripe_num; i++){

	for(j=0; j<num_chunks_in_stripe; j++)
		fprintf(fd, "%d ", chunk_map[i*num_chunks_in_stripe+j]);

	fprintf(fd, "\n");
   	}

   fclose(fd);


   free(flag_index);
   free(chunk_map);


}


int main(int argc, char** argv){

  gen_chunk_distrbtn();

}

