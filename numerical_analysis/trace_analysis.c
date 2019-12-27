#define _GNU_SOURCE

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
#define m 4
#define n 16
#define node_num 16
#define rack_num 4
#define nodes_per_rack node_num/rack_num

#define cau_replica_num 1
#define chunk_size 1024*1024
#define stripe_num 5000000
#define bucket_len 100000
#define bucket_width 1000
#define cau_log_strps 100
#define strlen  100
#define UPPBND  9999

#define run_dl 1
#define run_baseline 2
#define run_cau 3

int pl_sum_cross_traffic; 
int parix_sum_cross_traffic;
int cau_sum_cross_traffic; 
int commit_approach;
int write_count;
int cau_replica_traffic;
int cau_spu_traffic;
int cau_hdg_traffic;

int bucket[bucket_len*bucket_width];
int cau_updt_tab[cau_log_strps*(k+1)]; //it records the updates in the logging stage
int chunk_map[stripe_num*n];
//int nodes_in_racks[rack_num]={nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack};
//int nodes_in_racks[rack_num]={nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack};
int nodes_in_racks[rack_num]={nodes_per_rack, nodes_per_rack, nodes_per_rack, nodes_per_rack};


void print_array(int row, int col, int *array){

  int i,j; 

  for(i=0; i<row; i++){

	for(j=0; j<col; j++)
		printf("%d ", array[i*col+j]);

	printf("\n");

  	}
}

int find_max_array_index(int* array, int num){

	int i;
	int ret=-1;
	int index;

	for(i=0; i<num; i++){

		if(array[i]>ret){
			ret=array[i];
			index=i;
			}

		}

	return index;
}


int find_none_zero_min_array_index(int* array, int num, int exception){

	int i;
	int ret=9999;
	int index=-1;
	int max_updt_freq=-1;

	for(i=0; i<num; i++){

		if(i==exception)
			continue;

		if(array[i]==0)
			continue;

		if(array[i]<ret){
			ret=array[i];
			index=i;
			}

		}

	return index;
}



//given a node, return the rack_id it resides in
int get_rack_id(int node_id){

    int i;
    int count;

    count=0;
    for(i=0; i<rack_num; i++){

        count+=nodes_in_racks[i];

        if(count>node_id)
            break;

    }

    return i;

}


// sort the data access frequencies in descending order
void quick_sort(int* data, int* index, int start_id, int end_id){

	int left=start_id;
	int right=end_id;

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


void nmrcl_pfrm_chnk_movmt(int in_chunk_id, int out_chunk_id){

	//update chunk_map
	int temp;
	temp=chunk_map[in_chunk_id];
	chunk_map[in_chunk_id]=chunk_map[out_chunk_id];
	chunk_map[out_chunk_id]=temp;
	
}

//we assume that the data are separated for each commit 
void hot_cold_separation(){

  //printf("Hot_cold_separation starts:\n");
  int i;
  int j;
  int temp_rack_id;
  int slct_rack;
  int h;
  int search_start;
  int in_chunk_id, in_chnk_nd_id;
  int out_chunk_id, out_chnk_nd_id;
  int incre_crs_trfic;
  int node_id;
  int stripe_id;
  int chunk_id;
  int l, prty_rack_id;
  int orig_cmmt_cost, new_cmmt_cost;
  int cddt_rack_id;
  int its_rack_id;
  int max_prty_rack;
  
  int temp_dt_updt_freq_stripe[k];
  int temp_dt_chnk_index[k];
  int rcd_rack_id[rack_num];
  int rack_prty_num[rack_num];

  incre_crs_trfic=0;

  //printf("cau_updt_tab:\n");
  //print_array(cau_log_strps, k+1, cau_updt_tab);
 
  //sort each stripe 
  for(i=0; i<cau_log_strps; i++){

	//printf("--stripe_id=%d\n", cau_updt_tab[i*(k+1)]);

	if(cau_updt_tab[i*(k+1)]==-1)
		break;

	for(j=0; j<k; j++)
		temp_dt_chnk_index[j]=j;

	for(j=0; j<k; j++){

		if(cau_updt_tab[i*(k+1)+j+1]>=0)
			temp_dt_updt_freq_stripe[j]=1;

		else 
			temp_dt_updt_freq_stripe[j]=-1;
		
		}

	//sort the data chunks with their indices
	quick_sort(temp_dt_updt_freq_stripe, temp_dt_chnk_index, 0, k-1);

	memset(rcd_rack_id, 0, sizeof(int)*rack_num);
	memset(rack_prty_num, 0, sizeof(int)*rack_num);

	//find where the rack that has most updated chunks 
	for(j=0; j<k; j++){

        //we only consider the chunks that are accessed
		if(temp_dt_updt_freq_stripe[j]==-1)
			break;

		stripe_id=cau_updt_tab[i*(k+1)];
		chunk_id=temp_dt_chnk_index[j];
		node_id=chunk_map[cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[j]];

		temp_rack_id=get_rack_id(node_id);
		rcd_rack_id[temp_rack_id]++;

		}
	
	//record the number of parity chunks in racks
	for(l=0; l<m; l++){
	
		stripe_id=cau_updt_tab[i*(k+1)];
		node_id=chunk_map[stripe_id*n+k+l];
		prty_rack_id=get_rack_id(node_id); 
	
		rack_prty_num[prty_rack_id]++;
	
	}

	//locate the destine rack id that has the maximum number of update chunks
	slct_rack=find_max_array_index(rcd_rack_id, rack_num);
	cddt_rack_id=find_none_zero_min_array_index(rcd_rack_id, rack_num, slct_rack);

    //if there is only one rack with updated data 
	if(cddt_rack_id==-1){

        //if there are more than two chunks updated in the selected rack, then do not move
		if(rcd_rack_id[slct_rack]>1)
			continue;
		
		//we can place the single hot data chunk to the rack where there are most parity chunks 
		max_prty_rack=find_max_array_index(rack_prty_num, rack_num);

        //if the selected rack has parity chunks, then do not move 
		if(rack_prty_num[slct_rack]>0)
			continue;

		//printf("max_prty_rack=%d\n", max_prty_rack);

        //locate the hot chunk
		for(h=0; h<k; h++){

			if(temp_dt_updt_freq_stripe[h]==1)
				break;
			
			}

		in_chunk_id=cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[h];

        //find a cold chunk in max_prty_rack
		for(h=0; h<k; h++){

			if(temp_dt_updt_freq_stripe[h]==1)
				continue;

			temp_rack_id=get_rack_id(chunk_map[cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[h]]);

			if(temp_rack_id==max_prty_rack){

				out_chunk_id=cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[h];
				
				nmrcl_pfrm_chnk_movmt(in_chunk_id, out_chunk_id);
				cau_hdg_traffic+=2;
				cau_sum_cross_traffic+=2;
				incre_crs_trfic+=2;

				break;

				}

			}

		continue;

		}
	
	//perform separation for the racks with max and min number of update chunks 
	if(cddt_rack_id!=slct_rack){

        // we prefer the two racks that can group all their stored hot data chunks within a rack
		if(rcd_rack_id[cddt_rack_id]+rcd_rack_id[slct_rack]>nodes_per_rack-rack_prty_num[slct_rack])
			continue;

		orig_cmmt_cost=0;
		new_cmmt_cost=0;

		//the cost of committing the hot data chunks in the rack temp_rack_id before movement
		for(h=0; h<rack_num; h++){

			if(h==cddt_rack_id)
				continue;

			if(rcd_rack_id[cddt_rack_id]<rack_prty_num[h])
					orig_cmmt_cost+=rcd_rack_id[cddt_rack_id];

			else 
				orig_cmmt_cost+=rack_prty_num[h];

			}

		//the cost of committing the hot data chunks in the rack slct_rack_id before movement
		for(h=0; h<rack_num; h++){

			if(h==slct_rack)
				continue;

			if(rcd_rack_id[slct_rack]<rack_prty_num[h])
				orig_cmmt_cost+=rcd_rack_id[slct_rack];

			else 
				orig_cmmt_cost+=rack_prty_num[h];

			}

        //the cost after movement
		for(h=0; h<rack_num; h++){

			if(h==slct_rack)
				continue;

			if(rcd_rack_id[slct_rack]+rcd_rack_id[cddt_rack_id]<rack_prty_num[h])
				new_cmmt_cost+=rcd_rack_id[slct_rack]+rcd_rack_id[cddt_rack_id];

			else 
				new_cmmt_cost+=rack_prty_num[h];

			}

		if(new_cmmt_cost > orig_cmmt_cost-2*rcd_rack_id[cddt_rack_id])
			continue;

		//select a cold chunk from this rack and perform switch
		for(j=0; j<k; j++){

			if(temp_dt_updt_freq_stripe[j]==-1)
				break;

			its_rack_id=get_rack_id(chunk_map[cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[j]]);

			if(its_rack_id!=cddt_rack_id)
				continue;
			
			for(h=0; h<k; h++){

            	//we only move the chunks that are not updated 
				if(temp_dt_updt_freq_stripe[h]==1) 
					continue;

				//printf("temp_index[k]=%d\n",temp_index[k]);
				temp_rack_id=get_rack_id(chunk_map[cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[h]]);

				if(temp_rack_id==slct_rack){

					in_chunk_id=cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[j];
					out_chunk_id=cau_updt_tab[i*(k+1)]*n+temp_dt_chnk_index[h];

					out_chnk_nd_id=chunk_map[out_chunk_id];
					in_chnk_nd_id=chunk_map[in_chunk_id];
					
					nmrcl_pfrm_chnk_movmt(in_chunk_id, out_chunk_id);
					cau_sum_cross_traffic+=2;
					cau_hdg_traffic+=2;
					incre_crs_trfic+=2;

					break;
					
				}
			  }
		   }
		}
	}

  //printf("data_separation completes, incre_crs_trfic=%d\n", incre_crs_trfic);
  
}


//this function is to read the mapping information from disk and keep it in the memory 
void read_numercial_chunk_map(char* map_file){

	int j;
	char strline[strlen];
	int index;
	int stripe_id;
	int temp;

	memset(chunk_map, -1, sizeof(int)*stripe_num*n);

	FILE *fd=fopen(map_file, "r");
	if(fd==NULL)
		perror("Error");

    stripe_id=0;
	temp=0;
	while(fgets(strline, strlen, fd)!=NULL){

	  //printf("%s",strline);
	  index=0;

	  for(j=0; j<strlen; j++){

		if(strline[j]=='\0')
			break;

		if(strline[j]>='0' && strline[j]<='9')
			temp=temp*10+strline[j]-'0';

		if(strline[j]==' '){
			chunk_map[stripe_id*n+index]=temp;
			//printf("global_chunk_map[%d][%d]=%d\n", stripe_id, index, chunk_map[stripe_id*n+index]);
			temp=0;
			index++;
			}
		}
	  
	  stripe_id++;
	}

	fclose(fd);

	//print_array(stripe_num, n, chunk_map);
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


int pl_cross_traffic(int start_chunk, int end_chunk){

    int i;
	int j;
	int stripe_id;
	int chunk_id;
	int rack_id;
	int node_id;
	int prty_rack_id;
	int sum; 

	sum=0;
	//for each updated chunk, locate its rack_id and the rack_ids of m parity chunks
	for(i=start_chunk; i<=end_chunk; i++){

		stripe_id=i/k;
		chunk_id=i%k;
		node_id=chunk_map[stripe_id*n+chunk_id];
		rack_id=get_rack_id(node_id);

		//printf("data chunk: stripe_id=%d, data_chunk_id=%d, node_id=%d, rack_id=%d\n", stripe_id, chunk_id, node_id, rack_id);
		//locate the rack_ids of parity chunks 
		for(j=0; j<m; j++){

			node_id=chunk_map[stripe_id*n+k+j];
			prty_rack_id=get_rack_id(node_id);
			//printf("parity chunk: parity_chunk_id=%d, prty_node_id=%d, prty_rack_id=%d\n", k+j, node_id, prty_rack_id);

			if(rack_id!=prty_rack_id)
				sum++;

			}

		//printf("sum=%d\n",sum);
		}
	
	return sum;

}


int check_if_first_updt(int chunk_id){

	int bucket_id;
	int i;
	int flag;

	bucket_id=chunk_id%bucket_len;

	for(i=0; i<bucket_width; i++){
		if(bucket[i*bucket_len+bucket_id]==chunk_id){
			flag=0;
			break;
			}
		else if(bucket[i*bucket_len+bucket_id]==-1){
			flag=1;
			break;
			}
		}

	if(i>=bucket_width){
		printf("ERR: bucket is full!\n");
		exit(1);
		}

    //if it is a new chunk, then record it
	if(flag==1)
		bucket[i*bucket_len+bucket_id]=chunk_id;

	return flag;

}


// parix approach
int parix(int start_chunk, int end_chunk){

    int i;
	int j;
	int stripe_id;
	int chunk_id;
	int rack_id;
	int node_id;
	int prty_rack_id;
	int sum; 
	int if_first_updt;
	int temp_sum;

	sum=0;
	for(i=start_chunk; i<=end_chunk; i++){

		stripe_id=i/k;
		chunk_id=i%k;
		node_id=chunk_map[stripe_id*n+chunk_id];
		rack_id=get_rack_id(node_id);

		//printf("stripe_id=%d, chunk_id=%d, node_id=%d, rack_id=%d\n", stripe_id, chunk_id, node_id, rack_id);
		//locate the rack_id of parity chunks
		temp_sum=0;
		for(j=0; j<m; j++){

			node_id=chunk_map[stripe_id*n+k+j];
			prty_rack_id=get_rack_id(node_id);

			if(rack_id!=prty_rack_id)
				temp_sum++;
			
			}

		sum+=temp_sum;

		//check if the chunk is udpated for the first time
		if_first_updt=check_if_first_updt(i);

        //if it is updated for the first time, the old version should be kept in the parity chunks
		if(if_first_updt==1)
			sum+=temp_sum;
		
		}

	return sum;

}


int cau_commit(int if_end_trace){

	 int h;
	 int l,p;
	 int stripe_id;
	 int node_id;
	 int prty_rack_id;
	 int rack_id;
	 int sum;
	 int if_rack_updt[rack_num];
	 int rack_prty_num[rack_num];

	 sum=0;

	 //for each logged stripe, determine the rack of each updated chunk
	 for(h=0; h<cau_log_strps; h++){

           //in the commit when the trace is finished, the number of logged stripes may be less than cau_log_strps
		if(cau_updt_tab[h*(k+1)]==-1)
			break;

		memset(if_rack_updt, 0, sizeof(int)*rack_num);
		memset(rack_prty_num, 0, sizeof(int)*rack_num);

        //record the number of updated chunks in racks
		for(l=0; l<k; l++){

			if(cau_updt_tab[h*(k+1)+l+1]>=0){

				stripe_id=cau_updt_tab[h*(k+1)];
				node_id=chunk_map[stripe_id*n+l];
				rack_id=get_rack_id(node_id);

				if_rack_updt[rack_id]++;
						
				}
			}

		//record the number of parity chunks in racks
			for(l=0; l<m; l++){

				stripe_id=cau_updt_tab[h*(k+1)];
				node_id=chunk_map[stripe_id*n+k+l];
				prty_rack_id=get_rack_id(node_id); 

				rack_prty_num[prty_rack_id]++;

				}
			
			//if direct commit and selective commit
			for(l=0; l<rack_num; l++){
				for(p=0; p<rack_num; p++){

					if(p==l)
						continue;

                    //if direct commit
					if(commit_approach==1)
						sum+=if_rack_updt[l]*rack_prty_num[p];

                    //if selective commit
                    else if(commit_approach==2 || commit_approach==3){
						
						if(if_rack_updt[l]<rack_prty_num[p])
							sum+=if_rack_updt[l];
						
						else 
							sum+=rack_prty_num[p];
                    	}
					}
				}
			}


	 return sum;

}


// we commit the parity delta when the number of updated stripes reaches 20
void cau_cross_traffic(int start_chunk, int end_chunk, int if_end_trace){

    int i;
    int j;
    int stripe_id;
    int chunk_id;
    int rack_id;
    int node_id;
    int prty_rack_id;
    int sum_direct_cmmt, sum_selective_cmmt; 
	int sum_hot_grouping;
    int if_first_updt;
	int h;
	int l;
	int p;
	int log_prty_id;
	int log_prty_rack_id;
    int index;
	int its_prty_nd_id;
	int its_rack_id;
	int sum;
	int temp_sum;

	sum=0;
    sum_direct_cmmt=0;
	sum_selective_cmmt=0;
	sum_hot_grouping=0;

	j=-1;

	if(start_chunk==-1){
		
		temp_sum=cau_commit(if_end_trace);
		sum+=temp_sum;
		cau_spu_traffic+=temp_sum;
		cau_sum_cross_traffic+=temp_sum;
		
		}

	for(i=start_chunk; i<=end_chunk; i++){

		stripe_id=i/k;
		chunk_id=i%k; 
		node_id=chunk_map[stripe_id*n+chunk_id];
		rack_id=get_rack_id(node_id);

		if(stripe_id>stripe_num){

			printf("stripe num is too small, stripe_id=%d, stripe_num=%d\n", stripe_id, stripe_num);
			exit(1);

			}

		//printf("data_chunk: stripe_id=%d, chunk_id=%d, node_id=%d, rack_id=%d\n", stripe_id, chunk_id, node_id, rack_id);
		for(j=0; j<cau_log_strps; j++){

            //if the stripe has been updated, then mark the chunk_id
			if(cau_updt_tab[j*(k+1)]==stripe_id){
				
				cau_updt_tab[j*(k+1)+chunk_id+1]++;
				break;
				
				}

            //if the stripe has not been recorded, then record it
			if(cau_updt_tab[j*(k+1)]==-1){
				
				cau_updt_tab[j*(k+1)]=stripe_id;
				cau_updt_tab[j*(k+1)+chunk_id+1]++;
				break;

				}
			}

        // if we keep a replica for each update
        if(cau_replica_num>0){
			cau_replica_traffic+=cau_replica_num;
			cau_sum_cross_traffic+=cau_replica_num;
        	}

		//if the table is full or the trace is finished, then commit the parity deltas
		if(j>=cau_log_strps){

		    temp_sum=cau_commit(if_end_trace);
			cau_spu_traffic+=temp_sum;
			cau_sum_cross_traffic+=temp_sum;

			//if it is not the end of the trace, then perform separation
			if(commit_approach==3 && if_end_trace!=1)
				hot_cold_separation();

			//reset the default setting after commit
			memset(cau_updt_tab, -1, sizeof(int)*cau_log_strps*(k+1));
			
			}

		}
   
}


//read a trace
void trace_analysis_process(char* trace_name, int update_scheme){

    //read the chunk_map
	if(n==9)
	   read_numercial_chunk_map("cau_chunk_map_n9k6");
	
	else if(n==14)
	   read_numercial_chunk_map("cau_chunk_map_n14k10");
	
	else if(n==16)
	   read_numercial_chunk_map("cau_chunk_map_n16k12");

	else if(n==17 && k==13)
	   read_numercial_chunk_map("cau_chunk_map_n17k13");
	
	else if(n==18 && k==14)
	   read_numercial_chunk_map("cau_chunk_map_n18k14");

	else if(n==19 && k==15)
	   read_numercial_chunk_map("cau_chunk_map_n19k15");

	else if(n==20 && k==16)
	   read_numercial_chunk_map("cau_chunk_map_n20k16");

	else printf("ERR: read_chunk_map!\n");

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        perror("Error");
        exit(1);
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
    int op_size;
    int ret;
	int access_start_stripe, access_end_stripe;
	int access_start_chunk, access_end_chunk;
	int num_chunks_updated;
	int read_count;

    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;

	read_count=0;
	write_count=0;
	
	pl_sum_cross_traffic=0;
	parix_sum_cross_traffic=0;
	cau_sum_cross_traffic=0;

	cau_replica_traffic=0;
	cau_spu_traffic=0;
	cau_hdg_traffic=0;

    struct timeval bg_tm, ed_tm; 

	num_chunks_updated=0;
	
    while(fgets(operation, sizeof(operation), fp)) {
		
        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
		new_strtok(operation,divider,volumn_id);
		new_strtok(operation,divider,op_type);
		new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
		new_strtok(operation,divider,usetime);

        if(strcmp(op_type, "Read")==0){
			read_count++;
			continue;
        	}

		else if(strcmp(op_type, "Write")==0)
			write_count++;

        trnsfm_char_to_int(offset, offset_int);
        trnsfm_char_to_int(size, size_int);

        access_start_chunk=(*offset_int)/((long long)(chunk_size));
        access_end_chunk=(*offset_int+*size_int-1)/((long long)(chunk_size));

        access_start_stripe=access_start_chunk/k;
        access_end_stripe=access_end_chunk/k;

		num_chunks_updated+=access_end_chunk-access_start_chunk+1;

		if(access_end_stripe>stripe_num){

			printf("ERR: stripe_num too small!, access_end_stripe=%d\n", access_end_stripe);
			exit(1);

			}

		if(update_scheme==run_baseline)
			pl_sum_cross_traffic += pl_cross_traffic(access_start_chunk, access_end_chunk);

		else if(update_scheme == run_dl)
			parix_sum_cross_traffic += parix(access_start_chunk, access_end_chunk);

		else cau_cross_traffic(access_start_chunk, access_end_chunk, 0);
		

    }

	//printf("write_count=%d, ", write_count);

	//in the end of the trace, cau will also commit the deltas 
	if(update_scheme==run_cau)
		cau_cross_traffic(-1, -1, 1);

	if(update_scheme==run_baseline)
		printf("baseline_sum_cross_traffic=%d\n", pl_sum_cross_traffic);

	else if(update_scheme==run_dl)
		printf("parix_sum_cross_traffic=%d\n", parix_sum_cross_traffic);

	else if (update_scheme==run_cau)
		printf("cau_sum_cross_traffic=%d\n", cau_sum_cross_traffic);

	//printf("%d\n", cau_spu_traffic + cau_hdg_traffic);
	
	//printf("Trace: %s, pl_sum_cross_traffic=%d, parix_sum_cross_traffic=%d, cau_sum_cross_traffic=%d\n", trace_name, pl_sum_cross_traffic, parix_sum_cross_traffic, cau_sum_cross_traffic);

    fclose(fp);

}


int main(int argc, char** argv){

    if(argc!=3){
        printf("./trace_analysis trace if_hot_grouping!\n");
        exit(0);
    }

    commit_approach = atoi(argv[2]);
    printf("commit_approach = %d\n", commit_approach);

    if(commit_approach==1)
        printf("Append Direct Commit\n");

    else if(commit_approach==2)
        printf("Selective Commit\n");

    else if(commit_approach==3)
   	    printf("Selective Commit plus Hot Grouping\n");

    else {
        printf("input error!\n");
        exit(1);
    }

   memset(bucket, -1, sizeof(int)*bucket_len*bucket_width);
   memset(cau_updt_tab, -1, sizeof(int)*cau_log_strps*(k+1));

   trace_analysis_process(argv[1], run_baseline);
   trace_analysis_process(argv[1],run_dl);
   trace_analysis_process(argv[1],run_cau);

   return 0;
}

