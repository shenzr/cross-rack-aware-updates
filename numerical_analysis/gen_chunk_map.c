//This program is to generate the chunk_map for different pairs of n and k in numerical analysis 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <math.h>

#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>
#include <arpa/inet.h>

#define k 12
#define m 5
#define n 17
#define node_num 20
#define rack_num 5
#define stripe_num 1000000

int chunk_map[stripe_num*n];

void gene_mul_strps(){

   	int rank;
   	int i;
   	int j;
   	int base;
	int node_per_rack = node_num/rack_num;

   	int* chunk_to_node=(int*)malloc(sizeof(int)*n);
   	int* flag_index=(int*)malloc(sizeof(int)*node_num);
	int* rack_chunk_cnt = (int*)malloc(sizeof(int)*rack_num);

   	srand((unsigned int)time(0));
   	for(i=0; i<stripe_num; i++){

		memset(chunk_to_node, -1, sizeof(int)*n);
		memset(rack_chunk_cnt, 0, sizeof(int)*rack_num);

	 	for(j=0; j<node_num; j++)
	 		flag_index[j]=j;

	 	base=node_num;
	 	for(j=0; j<n; j++){
			//printf("j=%d, n=%d\n", j, n);
			rank=rand()%base;
			// promise rack-level fault tolerance
			if(rack_chunk_cnt[flag_index[rank]/node_per_rack]==m){
				j--;
				continue;
			}

			chunk_to_node[j]=flag_index[rank];
			flag_index[rank]=flag_index[node_num-j-1];
			flag_index[node_num-j-1]=-1;
			rack_chunk_cnt[chunk_to_node[j]/node_per_rack]++;
			base--;
	 	}

	 	for(j=0; j<n; j++)
	 		chunk_map[i*n+j]=chunk_to_node[j];
   	}

    //write the mapping info to a chunk_2_node 
    FILE *fd; 

    if(n==16 && k==12)
        fd = fopen("cau_chunk_map_n16k12","w");
    else if(n==9 && k==6)
        fd = fopen("cau_chunk_map_n9k6", "w");
    else if(n==14 && k==10)
        fd = fopen("cau_chunk_map_n14k10", "w");
    else {
        fd = fopen("cau_chunk_map", "w");
    }

    for(i=0; i<stripe_num; i++){
		for(j=0; j<n; j++)
			fprintf(fd, "%d ", chunk_map[i*n+j]);

		fprintf(fd, "\n");
   	}

   fclose(fd);
   

   	free(chunk_to_node);
	free(flag_index);
	free(rack_chunk_cnt);
}


int main(int argc, char** argv){

   if(argc!=1){
     printf("./gen_numerical_chunk_map !\n");
     exit(0);
    }
	printf("n=%d\n",n);
	gene_mul_strps();

   return 0;
}

