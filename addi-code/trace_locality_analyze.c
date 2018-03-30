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

#define distin_chnk_num 10000
#define bucket_num      100
#define entry_num       100
#define chunk_size      1024*1024

#define data_chunks     6

int analyze_count;
int aver_locality;

int distin_count;
int distinct_stripe_bucket[distin_chnk_num];

/* this function truncate a component from a string according to a given divider */
void new_strtok(char string[], char divider, char result[]){

    int i,j;

    for(i=0;string[i]!='\0';i++){

        if(string[i]!=divider)
            result[i]=string[i];

        else break;

    }

    // if the i reaches the tail of the string
    if(string[i]=='\0')
        result[i]='\0';

    // else it finds the divider
    else {

        // seal the result string
        result[i]='\0';

        // shift the string and get a new string
        for(j=0;string[j]!='\0';j++)
            string[j]=string[j+i+1];

    }
}


//this function transforms a char type to an integer type
void trnsfm_char_to_int(char *char_data, long long *data){

    int i=0;
    *data=0LL;

    while(char_data[i]!='\0'){
        if(char_data[i]>='0' && char_data[i]<='9'){
            (*data)*=10;
            (*data)+=char_data[i]-'0';
        }
        i++;
    }
}



void check_chunk(int stripe_id){

	int bucket_id;
	int i;
	int if_find;

	bucket_id=stripe_id%bucket_num;
	if_find=0;

	//printf("bucket_id=%d\n", bucket_id);

	//check if the chunk is in the bucket 
	for(i=0; i<entry_num; i++){

		if(distinct_stripe_bucket[bucket_id*entry_num+i]==-1)
			break;

		if(distinct_stripe_bucket[bucket_id*entry_num+i]==stripe_id){
			if_find=1;
			break;
			}

		}

	if(i>=entry_num){

		printf("ERR: bucket-%d is full\n", bucket_id);
		exit(1);

		}

	if(if_find==0){
		distinct_stripe_bucket[bucket_id*entry_num+i]=stripe_id;
		distin_count++;
		}

}

//read a trace
void read_trace(char *trace_name, int analyze_gran){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(0);
    }


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
    int access_chunk_num;
    int ret;
    int count;
	int access_start_stripe, access_end_stripe;


    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;
    count=0;
	
    while(fgets(operation, sizeof(operation), fp)) {

        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
		new_strtok(operation,divider,volumn_id);
		new_strtok(operation,divider,op_type);
		new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
		new_strtok(operation,divider,usetime);

        if((ret=strcmp(op_type, "Read"))==0)
            continue;

        count++;

		if(count==analyze_gran){

			//reset info
			analyze_count++;
			aver_locality+=distin_count;
			
			distin_count=0;
			memset(distinct_stripe_bucket, -1, sizeof(int)*distin_chnk_num);

			count=0;
			}

        //printf("\n\n\ncount=%d, op_type=%s, offset=%s, size=%s\n", count, op_type, offset, size);

        // printf("cur_rcd_idx=%d\n",cur_rcd_idx);
        // analyze the access pattern
        // if it is accessed in the same timestamp

        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));

		access_start_stripe=access_start_chunk/data_chunks;
		access_end_stripe=access_end_chunk/data_chunks;

		//printf("access_start_chunk=%d, access_end_chunk=%d\n", access_start_chunk, access_end_chunk);

		for(i=access_start_stripe; i<=access_end_stripe; i++)
			check_chunk(i);

    }

	//for the end of the file 
	aver_locality+=distin_count;
	analyze_count++;

	printf("aver_locality=%d, analyze_count=%d\n", aver_locality, analyze_count);
	printf("Trace-%s: locality_ratio=%lf\n", trace_name, aver_locality*1.0/analyze_count);

	aver_locality=0;
	analyze_count=0;

    fclose(fp);

}


//this program is to analyze the trace locality
int main(int argc, char** argv){

   if(argc!=2){
    printf("input error: ./trace_analyze analyze_granularity!\n");
    exit(1);
  }
   
   int analyze_gran=atoi(argv[1]);

   char* trace[36]={"wdev_1.csv","wdev_2.csv","rsrch_1.csv","wdev3.csv","prxy_0.csv","rsrch_0.csv","prn_0.csv","src2_0.csv","mds_0.csv","proj_0.csv","stg_0.csv","ts_0.csv","wdev_0.csv","src1_2.csv", "web_0.csv","src2_2.csv","web_3.csv","hm_0.csv","usr_0.csv", "web_1.csv","src1_0.csv","stg_1.csv","prxy_1.csv","rsrch_2.csv", "prn_1.csv","usr_2.csv","proj_2.csv","proj_1.csv","usr_1.csv","mds_1.csv","proj_3.csv","src1_1.csv","hm_1.csv","src2_1.csv","proj_4.csv","web_2.csv"}
;
   int i;
   for(i=0; i<36; i++){

		analyze_count=0;
		aver_locality=0;
   	    distin_count=0;  
		
        memset(distinct_stripe_bucket, -1, sizeof(int)*distin_chnk_num);
        read_trace(trace[i], analyze_gran);
   }
}
