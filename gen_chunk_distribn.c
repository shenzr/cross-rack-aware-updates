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
//the number of chunks in a rack should not exceed max_chunks_per_rack
void init_parix_fo_gen_chunk_distrbtn(){

    int i,j;
    int base;
    int rank;
    int node_id;
    int rack_id;

    int* chunk_to_node=(int*)malloc(sizeof(int)*num_chunks_in_stripe);
    int* chunk_map=(int*)malloc(sizeof(int)*stripe_num*num_chunks_in_stripe); // maps chunk_id to node_id
    int* flag_index=(int*)malloc(sizeof(int)*total_nodes_num);
    int* num_chunk_in_rack=(int*)malloc(sizeof(int)*rack_num);

    srand((unsigned int)time(0));

    for(i=0; i<stripe_num; i++){

        memset(chunk_to_node, -1, sizeof(int)*num_chunks_in_stripe);
        memset(num_chunk_in_rack, 0, sizeof(int)*rack_num);

        //generate the distribution of each stripe randomly

        for(j=0; j<total_nodes_num; j++)
            flag_index[j]=j;

        base=total_nodes_num;

        for(j=0; j<num_chunks_in_stripe; j++){

            rank=rand()%base;
            node_id=flag_index[rank];
            rack_id=get_rack_id(node_id);

            if(num_chunk_in_rack[rack_id]>=max_chunks_per_rack){
                j--;
                continue;
            }

            chunk_to_node[j]=node_id;
            flag_index[rank]=flag_index[total_nodes_num-j-1];
            num_chunk_in_rack[rack_id]++;

            base--;

        }

        //printf("%d-th stripe node_map:\n",i);
        for(j=0; j<num_chunks_in_stripe; j++)
            chunk_map[i*num_chunks_in_stripe+j]=chunk_to_node[j];

    }

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
    free(num_chunk_in_rack);


}


int main(int argc, char** argv){

    init_parix_fo_gen_chunk_distrbtn();

    return 0;

}

