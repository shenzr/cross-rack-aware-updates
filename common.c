#define _GNU_SOURCE 

#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>
#include <sys/time.h>

#include "config.h"

/* it records the public ip address of amazon vms*/
char* node_ip_set[total_nodes_num]={"13.125.237.65", "13.125.237.16", "13.209.3.43", "13.125.234.148", "13.125.252.243", //seoul nodes
									"54.169.215.182", "13.250.30.81", "54.254.200.139", "54.169.253.138", "13.229.215.19", //sgp nodes
									"13.211.92.92", "54.206.12.247", "13.210.59.174", "54.252.161.166", "52.63.192.168", //sydney nodes
									"54.250.246.253", "13.114.248.187", "13.230.239.222", "13.231.84.103", "54.249.114.24"}; //tokyo

/* it records the inner ip address of amazon vms read from eth0 */
char* inner_ip_set[total_nodes_num]={"172.31.25.191", "172.31.20.73", "172.31.21.113", "172.31.17.16", "172.31.22.187", //seoul region
									 "172.31.6.250 ", "172.31.12.190", "172.31.2.43", "172.31.0.62", "172.31.8.102", //sgp region
									 "172.31.10.41", "172.31.4.5", "172.31.3.205", "172.31.8.171", "172.31.1.151", //sydney region
									 "172.31.27.175", "172.31.29.63", "172.31.30.174", "172.31.16.112", "172.31.26.40"}; //tokyo region

char* mt_svr_ip="13.229.209.179";
char* client_ip="13.250.46.9";
char* gateway_ip="13.229.232.195";

/* we currently consider all the regions have the same number of nodes */
int   nodes_in_racks[rack_num]={node_num_per_rack, node_num_per_rack, node_num_per_rack, node_num_per_rack};

<<<<<<< HEAD
/* the four regions in our test */
char* region_name[rack_num]={"Seoul", "Singapore", "Sydney", "Tokyo"};


/*
 * a hash function to check the integrity of received data
 */ 
=======
char* region_name[rack_num]={"Seoul", "Singapore", "Sydney", "Tokyo"};


// a hash function to check the integrity of received data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
unsigned int RSHash(char* str, unsigned int len)
{
    unsigned int b    = 378551;
    unsigned int a    = 63689;
    unsigned int hash = 0;
    unsigned int i    = 0;

    for(i = 0; i < len; str++, i++)
    {
        hash = hash * a + (*str);
        a    = a * b;
    }

    return hash;
}

<<<<<<< HEAD
/*
 * Given an ip address, this function locates its node id
 */ 
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int get_node_id(char* given_ip){

	int i;
	int ret;

    //locate the node id
    for(i=0; i<total_nodes_num; i++){

        if((ret=strcmp(node_ip_set[i],given_ip))==0)
            break;

    }

    return i;

}


<<<<<<< HEAD
/*
 * This function returns the rack_id when given a node id based on the data center architecture
 */ 
=======
/* this function returns the rack_id of a given node_id */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
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


<<<<<<< HEAD
/*
 * print the region info and node info 
 */ 
=======
/* print the region info and node info */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void print_amazon_vm_info(char* node_ip){

	int node_id=get_node_id(node_ip);
	int rack_id=get_rack_id(node_id);
	int base=node_num_per_rack;
	
	printf("Region-%s, Node-%d\n", region_name[rack_id], node_id%base);

}

<<<<<<< HEAD
/*
 * print all the elements in an array
 */ 
=======
/* print all the elements in an array */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void print_array(int row, int col, int *array){

  int i,j; 

  for(i=0; i<row; i++){

	for(j=0; j<col; j++)
		printf("%d ", array[i*col+j]);

	printf("\n");

  	}
}

<<<<<<< HEAD
/*
 * calculate the sum of all the elements in an array
 */ 
=======
/* calculate the sum of all the elements in an array */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int sum_array(int num, int* arr){

	int count=0;
	int i;

	for(i=0; i<num; i++)
		count+=arr[i];

	return count;
}


<<<<<<< HEAD
/*
 * finds the maximum value of an array
 */ 
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int find_max_array(int* array, int n){

	int i;
	int ret=-1;

	for(i=0; i<n; i++){

		if(array[i]>ret)
			ret=array[i];

		}

	return ret;
}

<<<<<<< HEAD
/*
 * finds the index of the maximum value in an array
 */ 
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int find_max_array_index(int* array, int n){

	int i;
	int ret=-1;
	int index=-1;

	for(i=0; i<n; i++){

		if(array[i]>ret){
			ret=array[i];
			index=i;
			}

		}

	return index;
}


<<<<<<< HEAD
=======


>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
/* this function is executed by the metadata server, which 
 * reads the mapping information from a mapping file and keep the 
 * mapping info in the memory 
 */
void read_chunk_map(char* map_file){

	int j;
	char strline[strlen];
	int index;
	int stripe_id;
	int temp;

	FILE *fd=fopen(map_file, "r");
	if(fd==NULL)
		printf("open_file error!\n");

    stripe_id=0;
	temp=0;
	while(fgets(strline, strlen, fd)!=NULL){
		
	  index=0;

	  for(j=0; j<strlen; j++){

		if(strline[j]=='\0')
			break;

		if(strline[j]>='0' && strline[j]<='9')
			temp=temp*10+strline[j]-'0';

		if(strline[j]==' '){
			
			global_chunk_map[stripe_id*num_chunks_in_stripe+index]=temp;
			temp=0;
			index++;
			
			}
		}
	  
	  stripe_id++;
	}

<<<<<<< HEAD
	fclose(fd);
=======

	fclose(fd);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
}




<<<<<<< HEAD
/* 
 * this function reads the ip address from the nic eth0
 */ 
=======
/* this function reads the ip address from the nic eth0 */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void GetLocalIp(char* local_ip)
{

    int sock;
    struct sockaddr_in sin;
    struct ifreq ifr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);

    strncpy(ifr.ifr_name, "eth0", IFNAMSIZ);
    ifr.ifr_name[IFNAMSIZ-1]=0;

    if(ioctl(sock, SIOCGIFADDR, &ifr)<0)
        perror("ioctl");

    memcpy(&sin, &ifr.ifr_addr, sizeof(sin));

	close(sock);

	strcpy(local_ip, inet_ntoa(sin.sin_addr));

}


<<<<<<< HEAD
/*
 * this function gets the ip of the local node and returns the local_node_id in the node_ip_set
 */ 
=======
//this function gets the host_ip and returns the local_node_id in the node_ip_set
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int get_local_node_id(){

    char local_ip[ip_len];
    int i;
    int ret;

    GetLocalIp(local_ip);
<<<<<<< HEAD
=======
    //printf("server_ip=%s\n",local_ip);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

	if(if_gateway_open==1){

       //locate the node id
<<<<<<< HEAD
       for(i=0; i<total_nodes_num; i++)
        if((ret=strcmp(node_ip_set[i],local_ip))==0)
            break;
=======
       for(i=0; i<total_nodes_num; i++){

        if((ret=strcmp(node_ip_set[i],local_ip))==0)
            break;

		return i;

       }
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	 }

	else {

		for(i=0; i<total_nodes_num; i++)
			if(strcmp(local_ip, inner_ip_set[i])==0)
				break;
<<<<<<< HEAD
		}

	return i;
=======

		return i;

		}
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

}


<<<<<<< HEAD
/*
 * This function initializes a client socket 
 */ 
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int init_client_socket(int client_port_num){

    //set client_addr info
    struct sockaddr_in client_addr;
    bzero(&client_addr,sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_addr.s_addr = htons(INADDR_ANY);
    client_addr.sin_port = htons(client_port_num);

    //create client socket
    int client_socket = socket(AF_INET,SOCK_STREAM,0);
    int on=1;

    if(client_socket < 0)
    {
        perror("Create Socket Failed!\n");
<<<<<<< HEAD
        exit(1);
=======
        //exit(1);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    }

	setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    return client_socket;

}



<<<<<<< HEAD
/*
 * send data (e.g., data, cmd, ack) 
 */ 
=======
// this function is executed by the internal node, which send the aggregated data delta to the parity node for final encoding
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void send_data(TRANSMIT_DATA *td, char *server_ip, int port_num, ACK_DATA* ack, CMD_DATA* cmd, int send_data_type){

    int sent_len;
    int ret;
	int client_socket;
	int data_size;

<<<<<<< HEAD
    // judge the type of sent data
=======
	//printf("send_port=%d\n", port_num);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	if(send_data_type==UPDT_DATA)
		data_size=sizeof(TRANSMIT_DATA);

	else if(send_data_type==ACK_INFO)
		data_size=sizeof(ACK_DATA);

	else 
		data_size=sizeof(CMD_DATA);

    char* send_buff=(char*)malloc(data_size);

	if(send_data_type==UPDT_DATA)
		memcpy(send_buff, td, sizeof(TRANSMIT_DATA));
	
	else if(send_data_type==ACK_INFO)
		memcpy(send_buff, ack, sizeof(ACK_DATA));
	
	else
		memcpy(send_buff, cmd, sizeof(CMD_DATA));

<<<<<<< HEAD
	// send the data through socket
	client_socket=init_client_socket(0);

    // set server_addr info
=======
	//send the data through socket
	client_socket=init_client_socket(0);

    //set server_addr info
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    if(inet_aton(server_ip, &server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
		printf("Server IP is %s\n", server_ip);
        exit(1);
    }
    server_addr.sin_port = htons(port_num);

<<<<<<< HEAD
    while(connect(client_socket,(struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);
=======

    while(connect(client_socket,(struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);
    //printf("connect success!\n");
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    
    sent_len=0;
    while(sent_len < data_size){
        ret=write(client_socket, send_buff+sent_len, data_size-sent_len);
        sent_len+=ret;
    }

    free(send_buff);
	ret=close(client_socket);
	if(ret==-1)
		perror("close_send_data_socket error!\n");
<<<<<<< HEAD

=======
	
    //printf("send completes!\n");
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
}


// this function is executed by the internal node, which send the aggregated data delta to the parity node for final encoding
void send_req(UPDT_REQ_DATA* req, char* server_ip, int port_num, META_INFO* metadata, int info_type){

    int sent_len;
    int ret;
	int client_socket;
	int data_size;

<<<<<<< HEAD
	data_size=0;

=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	if(info_type==REQ_INFO)
		data_size=sizeof(UPDT_REQ_DATA);

	else if(info_type==METADATA_INFO)
		data_size=sizeof(META_INFO);

    char* send_buff=(char*)malloc(data_size);

	if(info_type==REQ_INFO)
		memcpy(send_buff, req, data_size);

	else if(info_type==METADATA_INFO)
		memcpy(send_buff, metadata, data_size);

	//send the data through socket
	client_socket=init_client_socket(0);

    //set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    if(inet_aton(server_ip, &server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
		printf("Server IP is %s\n", server_ip);
        exit(1);
    }
    server_addr.sin_port = htons(port_num);


    while(connect(client_socket,(struct sockaddr*)&server_addr, sizeof(server_addr)) < 0);
    
    sent_len=0;
    while(sent_len < data_size){
        ret=write(client_socket, send_buff+sent_len, data_size-sent_len);
        sent_len+=ret;
    }

    free(send_buff);

	ret=close(client_socket);
	if(ret==-1)
		perror("close_send_data_socket error!\n");
	
}

<<<<<<< HEAD
/*
 * This function initiates a server socket
 */
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int init_server_socket(int port_num){

	int server_socket;
    int opt=1;
	int ret;

    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htons(INADDR_ANY);
    server_addr.sin_port = htons(port_num);

    server_socket = socket(PF_INET,SOCK_STREAM,0);
    if( server_socket < 0){
        perror("Create Socket Failed!");
        exit(1);
    }

    ret=setsockopt(server_socket,SOL_SOCKET,SO_REUSEADDR,(char *)&opt,sizeof(opt)); //set the portnum reusable
	if(ret!=0){

		perror("setsockopt error!\n");
		exit(1);

		}

    if(bind(server_socket,(struct sockaddr*)&server_addr,sizeof(server_addr))){
        perror("Server Bind Port : Failed!");
        exit(1);
    }

	return server_socket;

}


//we use module as the hash algorithm
int update_loged_chunks(int given_chunk_id){

    int bucket_id;
    int i;
	int if_new_log_chunk=-1;

    bucket_id=given_chunk_id%bucket_num;

    // if the bucket is full
    if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*(entry_per_bucket-1)]>0){

        printf("Error! bucket_%d is full!\n", bucket_id);
        exit(0);

    }

    //scan the entries in that bucket
    for(i=0; i<entry_per_bucket; i++){

        // if find the given_chunk_id, udpate its log order
        if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]==given_chunk_id){
			if_new_log_chunk=0;
            break;
        	}

        // if reach the initialized ones
        if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]==-1){
			if_new_log_chunk=1;
            break;
        	}
    }

    // record the given_chunk_id
    newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]=given_chunk_id;
    newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i+1]=new_log_chunk_cnt;

    return if_new_log_chunk;

}


<<<<<<< HEAD
/*
 * this function transforms a char type to an integer type
 */ 
=======
/* this function transforms a char type to an integer type */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
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



<<<<<<< HEAD
/* 
 * this function truncates a component from a string according to a given divider 
 */
=======
/* this function truncate a component from a string according to a given divider */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
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



<<<<<<< HEAD
/* 
 * generate a random string for the given length
 */ 
void gene_radm_buff(char* buff, int len){

    int i;
=======
//generate a random string with the size of len
void gene_radm_buff(char* buff, int len){

    int i;
	
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    char alphanum[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for(i=0; i<chunk_size; i++)
        buff[i]=alphanum[i%(sizeof(alphanum)-1)];
<<<<<<< HEAD
}

/*
 * count the number of non-negatives in an array
 */ 
=======

}

//count the number of non-negatives in an array
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int count_non_ngtv(int* arr, int len){


    int i;
    int count;

    count=0;
    for(i=0; i<len; i++)
        if(arr[i]>=0)
            count++;

    return count;
}


<<<<<<< HEAD
/*
 * this function first finds the data chunk stored on this node when given a stripe_id
 * it then calculates the local_data_chunk_id and returns it
 */
=======
//given the nodes_in_racks, the set of ip addresses, this function gets the rack_id that the node resides in
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int get_local_chunk_id(int stripe_id){

    int local_node_id;
    int local_data_chunk_id;
    int i;

    local_node_id=get_local_node_id();

<<<<<<< HEAD
    // get the local_chunk_id
=======
	//printf("local_node_id=%d\n", local_node_id);

    //get the local_chunk_id
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    for(i=0; i<data_chunks; i++){

        if(global_chunk_map[stripe_id*num_chunks_in_stripe+i]==local_node_id)
            break;

    }

	if(i==data_chunks){

		printf("does not find the chunk\n");
		exit(1);
		
		}

<<<<<<< HEAD
	// we promise that the chunk_id exists, as we always select a node that has that chunk for partial encoding
    local_data_chunk_id=stripe_id*data_chunks+i; 
=======
    local_data_chunk_id=stripe_id*data_chunks+i; //we promise that the chunk_id exists, as we always select a node that has that chunk for partial encoding

    //printf("local_data_chunk_id=%d\n",local_data_chunk_id);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

    return local_data_chunk_id;

}




<<<<<<< HEAD
/*
 * given the chunk_map, this function records the store orders of all chunks on every node
 */
=======
//given the chunk_map, this function outputs the store order of each chunk on every node
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void get_chunk_store_order(){

    int i,j;
	int k;

	for(k=0; k<total_nodes_num; k++){
		
		num_store_chunks[k]=0;
		
        for(i=0; i<stripe_num; i++){
          for(j=0; j<num_chunks_in_stripe; j++){

<<<<<<< HEAD
			  // use "GLOBAL" order to represent store order of "ALL" chunks
              if(global_chunk_map[i*num_chunks_in_stripe+j]==k){

                chunk_store_order[k*max_num_store_chunks+num_store_chunks[k]]=i*num_chunks_in_stripe+j; 
=======
              if(global_chunk_map[i*num_chunks_in_stripe+j]==k){

                chunk_store_order[k*max_num_store_chunks+num_store_chunks[k]]=i*num_chunks_in_stripe+j; // use "GLOBAL" order to represent store order of ALL chunks
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
                num_store_chunks[k]++;

                if(num_store_chunks[k]>max_num_store_chunks) {

                    printf("exceed max_num_store_chunks\n");
                    exit(1);

                   }

               }
           }
       }
    }
}

<<<<<<< HEAD
/*
 * this function is to calculate the delta of two regions
 */
=======
// this function is to calculate the data delta of two regions
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void bitwiseXor(char* result, char* srcA, char* srcB, int length) {

    int i;
    int XorCount = length / sizeof(long);

    uint64_t* srcA64 = (uint64_t*) srcA;
    uint64_t* srcB64 = (uint64_t*) srcB;
    uint64_t* result64 = (uint64_t*) result;

    // finish all the word-by-word XOR
    for (i = 0; i < XorCount; i++) {
        result64[i] = srcA64[i] ^ srcB64[i];
    }

}


<<<<<<< HEAD
/* 
 * for each received chunk, this function adds them together and outputs a aggregated result
 */
=======
//for each receive data, perform XOR operations to them
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void aggregate_data(char* data_delta, int num_recv_chnks, char* ped){

	//printf("in aggregated_data:\n");

    int i;

    char tmp_buff[chunk_size];
    char tmp_data_delta[chunk_size];

    char* addrA;
    char* res;
    char* tmp;

    memcpy(tmp_data_delta, data_delta, chunk_size);

    addrA=tmp_data_delta;
    res=tmp_buff;

	//printf("start_addr=%x:\n",intnl_recv_data);

    for(i=0; i<num_recv_chnks; i++){
<<<<<<< HEAD
		
        bitwiseXor(res, addrA, ped+i*chunk_size*sizeof(char), chunk_size*sizeof(char));
=======

        //printf("res_addr=%p, addrA=%p\n", res, addrA);
        bitwiseXor(res, addrA, ped+i*chunk_size*sizeof(char), chunk_size*sizeof(char));
	    //printf("res=%x, addrA_addr=%x\n", tmp, addrA);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
        tmp=addrA;
        addrA=res;
        res=tmp;
    }
	
    memcpy(data_delta, addrA, chunk_size);

<<<<<<< HEAD
}

/*
 * This function writes the new data to the underlying disk
 */
=======
	//printf("out aggregated_data:\n");

}

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void flush_new_data(int stripe_id, char* new_data, int global_chunk_id, int stored_index){

    int fd;
    int ret;
    char* tmp_buff=NULL;
	
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
<<<<<<< HEAD
	if(ret){
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
		exit(1);
		}

	memcpy(tmp_buff, new_data, chunk_size);

    fd=open("data_file", O_RDWR);
    lseek(fd, stored_index*chunk_size, SEEK_SET);
	
    ret=write(fd, tmp_buff, chunk_size);
    if(ret!=chunk_size){
        printf("write data error!\n");
		exit(1);
    	}
=======
	if(ret)
		printf("ERROR: posix_memalign: %s\n", strerror(ret));

	memcpy(tmp_buff, new_data, chunk_size);

    struct timeval bg_tm, ed_tm;
	gettimeofday(&bg_tm, NULL);

    fd=open("data_file", O_RDWR);
    lseek(fd, stored_index*chunk_size, SEEK_SET);
    ret=write(fd, tmp_buff, chunk_size);
    if(ret!=chunk_size)
        printf("write data error!\n");
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

    close(fd);
	free(tmp_buff);

<<<<<<< HEAD
}

/*
 * This function listens an ack 
 */ 
void listen_ack(ACK_DATA* ack, char* recv_buff, int stripe_id, int updt_dt_id, int updt_prty_id, int port_num, int op_type){

=======
    gettimeofday(&ed_tm, NULL);
    //printf("flush_write_time=%lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}

void listen_ack(ACK_DATA* ack, char* recv_buff, int stripe_id, int updt_dt_id, int updt_prty_id, int port_num, int op_type){

   //printf("listen_ack:\n");

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   int recv_len; 
   int read_size;
   int ntf_connfd;
   int ntf_socket;
   int ret;

<<<<<<< HEAD
   // initialize socket information
   ntf_socket=init_server_socket(port_num);
   
   // initialize the sender info
=======

   //initial socket information
   ntf_socket=init_server_socket(port_num);
   
   //init the sender info
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   struct sockaddr_in sender_addr;
   socklen_t length=sizeof(sender_addr);
   
   if(listen(ntf_socket,100) == -1){
	   printf("Failed to listen.\n");
	   exit(1);
   }

   ntf_connfd = accept(ntf_socket, (struct sockaddr*)&sender_addr, &length);
<<<<<<< HEAD
=======
   //printf("receive ack from: ");
   //print_amazon_vm_info(inet_ntoa(sender_addr.sin_addr));
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

   recv_len=0; 
   while(recv_len<sizeof(ACK_DATA)){

	   read_size = read(ntf_connfd, recv_buff+recv_len, sizeof(ACK_DATA)-recv_len);
	   recv_len+=read_size;

<<<<<<< HEAD
=======
	   //printf("recv_len=%d, expected_size=%lu\n", recv_len, sizeof(TRANSMIT_DATA));

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	}

	close(ntf_connfd);
	ret=close(ntf_socket);
<<<<<<< HEAD
	if(ret==-1){
		perror("listen_ack: close_socket error!\n");
		exit(1);
		}

    memcpy(ack, recv_buff, sizeof(ACK_DATA));

}

/* 
 * the parity chunks in every stripe are encoded by using the same matrix
 * this function returns the encoding coefficient for a given chunk
 */
=======
	if(ret==-1)
		perror("listen_ack: close_socket error!\n");

    memcpy(ack, recv_buff, sizeof(ACK_DATA));

	//printf("op_type=%d, stripe_id=%d, updt_dt_id=%d, updt_prty_id=%d\n", op_type, stripe_id, updt_dt_id, updt_prty_id);

/*
    //listen ack for parity commit
    if((updt_dt_id==-1) && (cmt_ntf_td->op_type==op_type) && (cmt_ntf_td->stripe_id==stripe_id) && (cmt_ntf_td->updt_prty_id==updt_prty_id))
         printf("RECV_PRTY_CMMT_ACK: (stripe_id=%d, updt_parity_id=%d)\n", stripe_id, updt_prty_id);

    //listen ack for data update
	else if((updt_prty_id==-1) && (cmt_ntf_td->data_chunk_id==updt_dt_id) && (cmt_ntf_td->op_type==op_type))
		printf("RECV_DATA_UPDT_ACK: (stripe_id=%d, updt_dt_id=%d)\n", stripe_id, updt_dt_id);

    else {
       printf("RECV_ACK ERROR!\n");
       exit(1);
	   
    }
*/

}

//the parity chunks in every stripe are encoded by using the same matrix
//this function returns the encoding coefficient for a given chunk
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int obtain_encoding_coeff(int given_chunk_id, int prtyid_to_update){


    int index;

    index=given_chunk_id%data_chunks;

<<<<<<< HEAD
=======
	//printf("prty_to_update=%d, index=%d\n", prtyid_to_update, index);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    return encoding_matrix[prtyid_to_update*data_chunks+index];
}


<<<<<<< HEAD
/*
 * This encodes a data chunk based on the encoding coefficient of a given parity chunk
 */
void encode_data(char* data, char* pse_coded_data, int chunk_id, int updt_prty){

    int ecd_cef=obtain_encoding_coeff(chunk_id,updt_prty);
=======
void encode_data(char* data, char* pse_coded_data, int chunk_id, int updt_prty){

	//printf("encode_data works:\n");

    int ecd_cef=obtain_encoding_coeff(chunk_id,updt_prty);
	//printf("encod_marix[%d][%d]=encod_coeff=%d\n", updt_prty, chunk_id%data_chunks, ecd_cef);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    galois_w08_region_multiply(data, ecd_cef, chunk_size, pse_coded_data, 0);

}

<<<<<<< HEAD
/*
 * send ack to the destination node
 */
=======
//send ack to the destined node
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void send_ack(int stripe_id, int dt_id, int prty_id, char* destined_ip, int port_num, int op_type){

    int node_id; 
	int des_node_id;
	int rack_id;
	int des_rack_id;
<<<<<<< HEAD
	
	//initialize ack_data
=======

	//init the sent info
	
	//init ack_data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));

	ack->send_size=sizeof(ACK_DATA);
	ack->op_type=op_type;
	ack->stripe_id=stripe_id;
	ack->data_chunk_id=dt_id;
	ack->updt_prty_id=prty_id;
	ack->port_num=port_num;
	memcpy(ack->next_ip, destined_ip, ip_len);
<<<<<<< HEAD

	// if the destination node is the client node, then send the ack to the gateway if the gateway is opened 
=======
/*
	printf("ack->send_size=%d\n", ack->send_size);
	printf("ack->op_type=%d\n", ack->op_type);
	printf("ack->stripe_id=%d\n", ack->stripe_id);
	printf("ack->data_chunk_id=%d\n", ack->data_chunk_id);
	printf("ack->updt_prty_id=%d\n", ack->updt_prty_id);
	printf("ack->port_num=%d\n", ack->port_num);
	printf("ack->next_ip=%s\n", ack->next_ip);
*/

	//if the destined node is the metadata node, then send the ack to the gateway first 
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	if(strcmp(destined_ip, client_ip)==0){

		memcpy(ack->next_ip, client_ip, ip_len);

		if(if_gateway_open==1)
			send_data(NULL, gateway_ip, SERVER_PORT, ack, NULL, ACK_INFO);

		else 
			send_data(NULL, ack->next_ip, port_num, ack, NULL, ACK_INFO);
		
		return;

		}

<<<<<<< HEAD
    // determine the source node and the destination node and their racks
=======
    //determine the source node and the destination node and their racks
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	node_id=get_local_node_id();
	rack_id=get_rack_id(node_id);

	des_node_id=get_node_id(destined_ip);
	des_rack_id=get_rack_id(des_node_id);

<<<<<<< HEAD
    // if the gateway is opened and the two nodes are in different racks, then forward the data to the gateway first
    // else direct send the data to the destination node
=======
	//printf("send ack: node_id=%d, rack_id=%d, des_node_id=%d, des_rack_id=%d\n", node_id, rack_id, des_node_id, des_rack_id);

    //if the gateway is opened and the two nodes are in different racks, then forward the data to the gateway first
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	if((if_gateway_open==1) && (des_rack_id!=rack_id)){

		memcpy(ack->next_ip, destined_ip, ip_len);
		send_data(NULL, gateway_ip, SERVER_PORT, ack, NULL, ACK_INFO);

		}

	else 
		send_data(NULL, ack->next_ip, port_num, ack, NULL, ACK_INFO);
		
	
	free(ack);

<<<<<<< HEAD
}


/*
 * given a node_id and a global_chunk_id, return the storage index of that chunk on that node 
 * the storage index is to calculate the offset of the stored data on that node
 */
=======
	//printf("send_ack finishes\n");

}


//given a node_id and a global_chunk_id, return the store index of that chunk on that node 
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
int locate_store_index(int node_id, int global_chunk_id){

    int start_index, end_index;
	int mid_index;
	int store_index;
	
<<<<<<< HEAD
	// locate the stored order of given_chunk_id by using binary search
=======
	//locate the stored order of given_chunk_id by using binary search
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    start_index=0;
    end_index=num_store_chunks[node_id]-1;

    while(1){

        if(chunk_store_order[node_id*max_num_store_chunks+start_index]==global_chunk_id){

            store_index=start_index;
            break;

        }

        if(chunk_store_order[node_id*max_num_store_chunks+end_index]==global_chunk_id){

            store_index=end_index;
            break;

        }

        mid_index=start_index+(end_index-start_index)/2;

        if(chunk_store_order[node_id*max_num_store_chunks+mid_index]>global_chunk_id)
            end_index=mid_index-1;

        else if(chunk_store_order[node_id*max_num_store_chunks+mid_index]<global_chunk_id)
            start_index=mid_index+1;

        else if(chunk_store_order[node_id*max_num_store_chunks+mid_index]==global_chunk_id){

            store_index=mid_index;
            break;

        }

<<<<<<< HEAD
=======
		//printf("start_chunk=%d, end_chunk=%d\n", chunk_store_order[node_id*max_num_store_chunks+start_index], chunk_store_order[node_id*max_num_store_chunks+end_index]);
		//printf("global_chunk_id=%d\n", global_chunk_id);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
		if(start_index>=end_index){
			printf("search_global_chunk_id error!\n");
			exit(1);
			}
		
    }

	return store_index;

<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
}



<<<<<<< HEAD
/* 
 * receive the data and copy the buffer to a desinated area
 */
void* data_mvmnt_process(void* ptr){

=======
//receive the data and copy the buffer to a desinated area
void* data_mvmnt_process(void* ptr){

    //printf("data_mvmnt_process works:\n");

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

<<<<<<< HEAD
    // receive data
=======
    //receive data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-recv_len);
        recv_len+=read_size;

    }

<<<<<<< HEAD
    // copy the data 
=======
    //copy the data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

	if(td->prty_delta_app_role==IN_CHNK)
		memcpy(in_chunk, td->buff, chunk_size);

	else if(td->prty_delta_app_role==OUT_CHNK)
		memcpy(out_chunk, td->buff, chunk_size);

    free(td);
    free(recv_buff);

	return NULL;

}


<<<<<<< HEAD
/*
 * receive the data and copy the buffer to a destination area
 */
void* recv_data_process(void* ptr){

=======
//receive the data and copy the buffer to a desinated area
void* recv_data_process(void* ptr){

    //printf("recv_data_process works:\n");

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

<<<<<<< HEAD
    // receive data
=======
    //receive data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-recv_len);
        recv_len+=read_size;

    }

<<<<<<< HEAD
    // copy the data
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

	// copy the delta and record the data id
=======
    //copy the data
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

	//copy the delta and record the data id
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    memcpy(intnl_recv_data+chunk_size*sizeof(char)*rpd.recv_id, td->buff, chunk_size*sizeof(char));
	intnl_recv_data_id[td->data_chunk_id]=1;

    free(td);
    free(recv_buff);

	return NULL;

}


<<<<<<< HEAD
/* receive data by using multiple threads
 * if @flag_tag=1, then it performs recv_data_process
 * if @flag_tag=2, then it performs data_mvmnt_process
 */
void para_recv_data(int stripe_id, int num_recv_chnks, int port_num, int flag_tag){

=======
//receive data by using multiple threads
//if @flag_tag=1, then it performs recv_data_process
//if @flag_tag=2, then it performs data_mvmnt_process
void para_recv_data(int stripe_id, int num_recv_chnks, int port_num, int flag_tag){


    //printf("para_recv_data starts:\n");

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    int i;
    int server_socket;
    int max_connctn;
    int index;
	int ret;
    int* connfd=(int*)malloc(sizeof(int)*num_recv_chnks);

    max_connctn=100;
    index=0;
	
    pthread_t recv_data_thread[data_chunks];

	server_socket=init_server_socket(port_num);

<<<<<<< HEAD
    // initialize the sender info
=======
    //init the sender info
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    if(listen(server_socket,max_connctn) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	RECV_PROCESS_DATA* rpd=(RECV_PROCESS_DATA *)malloc(sizeof(RECV_PROCESS_DATA)*num_recv_chnks);
	memset(rpd, 0, sizeof(RECV_PROCESS_DATA)*num_recv_chnks);


    while(1){

        connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
<<<<<<< HEAD
=======
        //printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

        rpd[index].connfd=connfd[index];
        rpd[index].recv_id=index;

		if(flag_tag==1)
            pthread_create(&recv_data_thread[index], NULL, recv_data_process, (void *)(rpd+index));

		else 
			pthread_create(&recv_data_thread[index], NULL, data_mvmnt_process, (void *)(rpd+index));
        index++;

<<<<<<< HEAD
        if(index>=num_recv_chnks)
			break;
        	
    }

    for(i=0; i<num_recv_chnks; i++){
=======
        if(index>=num_recv_chnks){
			//printf("index>=num_recv_chunks\n");
			break;
        	}

    }

    for(i=0; i<num_recv_chnks; i++){
		//printf("waiting join: i=%d, num_recv_chnks=%d\n", i, num_recv_chnks);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
        pthread_join(recv_data_thread[i], NULL);
		close(connfd[i]);
    	}

    free(rpd);
	free(connfd);
	
	ret=close(server_socket);
<<<<<<< HEAD
	if(ret==-1){
	   perror("close_para_recv_data error!\n");
	   exit(1);
		}
	
}

/*
 * read old data from underlying disk for a given store_index
 */
=======
	if(ret==-1)
	   perror("close_para_recv_data error!\n");
	
	//printf("recv_completes\n");

}


>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void read_old_data(char* read_buff, int store_index){

    int ret;
    int fd;
	char* tmp_buff=NULL;

<<<<<<< HEAD
    // read the old data from data_file
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
	if(ret){
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
		exit(1);
		}
		
    fd=open("data_file", O_RDONLY);
    lseek(fd, store_index*chunk_size, SEEK_SET);
	
=======
    //read the old data from data_file
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
	if(ret)
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
		

    fd=open("data_file", O_RDONLY);
    lseek(fd, store_index*chunk_size, SEEK_SET);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    ret=read(fd, tmp_buff, chunk_size);
    if(ret!=chunk_size){
        printf("read data error!\n");
		exit(1);
    	}

<<<<<<< HEAD
=======
    //printf("read old data succeeds\n");
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	memcpy(read_buff, tmp_buff, chunk_size);

    close(fd);
	free(tmp_buff);

}

<<<<<<< HEAD
/*
 * in-place write the new data to an address specified by the store_index
 */
=======
//in-place write the new data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void write_new_data(char* write_buff, int store_index){

   int fd; 
   int ret; 
   char* tmp_buff=NULL;

<<<<<<< HEAD
   ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
   if(ret){
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
		exit(1);
   	}
   
   memcpy(tmp_buff, write_buff, chunk_size);

   // locate the operated offset
   fd=open("data_file", O_RDWR);
   lseek(fd, store_index*chunk_size, SEEK_SET);

   // write the new data 
   ret=write(fd, tmp_buff, chunk_size);
   if(ret!=chunk_size){
	   printf("write data error!\n");
	   exit(1);
   	}

=======
   struct timeval bg_tm, ed_tm;
   gettimeofday(&bg_tm, NULL);

   ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
   if(ret)
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
   
   memcpy(tmp_buff, write_buff, chunk_size);

   fd=open("data_file", O_RDWR);
   lseek(fd, store_index*chunk_size, SEEK_SET);
   ret=write(fd, tmp_buff, chunk_size);
   if(ret!=chunk_size)
	   printf("write data error!\n");

   //printf("write new data succeeds\n");
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   
   close(fd);
   free(tmp_buff);

<<<<<<< HEAD
}

/*
 * it generates a thread to send data 
 */ 
=======
   gettimeofday(&ed_tm, NULL);
   //printf("write_new_data_time=%lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}


>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void* send_updt_data_process(void* ptr){

   TRANSMIT_DATA td = *(TRANSMIT_DATA *)ptr;

<<<<<<< HEAD
=======
   printf("send_updt_data to: ");
   print_amazon_vm_info(td.sent_ip);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   send_data((TRANSMIT_DATA *)ptr, td.sent_ip, td.port_num, NULL, NULL, UPDT_DATA);

   return NULL;
}


<<<<<<< HEAD
/*
 * send the new data to the num_updt_prty parity nodes
 */
=======
//send the new data to the num_updt_prty parity chunks
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void para_send_dt_prty(TRANSMIT_DATA* td, int op_type, int num_updt_prty, int port_num){

   int j; 
   int prty_node_id;
   int prty_rack_id;
   int node_id;
   int rack_id;

<<<<<<< HEAD
   TRANSMIT_DATA* td_mt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*num_updt_prty);

   // get the rack_id of the local node
=======
   //brodcast the new data to m parity chunks
   TRANSMIT_DATA* td_mt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*num_updt_prty);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   node_id=get_local_node_id();
   rack_id=get_rack_id(node_id);
   
   pthread_t* parix_updt_thread=(pthread_t*)malloc(sizeof(pthread_t)*num_updt_prty);
   memset(parix_updt_thread, 0, sizeof(pthread_t)*num_updt_prty);

   for(j=0; j<num_updt_prty; j++){
   
<<<<<<< HEAD
	   // initialize td structure
=======
	   //init td structure
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	   td_mt[j].send_size=sizeof(TRANSMIT_DATA);
	   td_mt[j].op_type=op_type; 
	   td_mt[j].data_chunk_id=td->data_chunk_id;
	   td_mt[j].stripe_id=td->stripe_id;
	   td_mt[j].num_recv_chks_itn=-1;
	   td_mt[j].num_recv_chks_prt=-1;
	   td_mt[j].port_num=port_num;
	   td_mt[j].updt_prty_id=j;
	   memcpy(td_mt[j].buff, td->buff, sizeof(char)*chunk_size);
   
	   prty_node_id=td->updt_prty_nd_id[j]; 
	   prty_rack_id=get_rack_id(prty_node_id);

<<<<<<< HEAD
       // if send data across racks, we have to check if the gateway is opened
=======
	   //printf("prty_node_id=%d, prty_ip=%s\n", prty_node_id, node_ip_set[prty_node_id]);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	   if((if_gateway_open==1) && (rack_id!=prty_rack_id)){
		  memcpy(td_mt[j].sent_ip, gateway_ip, ip_len);
		  memcpy(td_mt[j].next_ip, node_ip_set[prty_node_id], ip_len);
	   	}
	   
	   else 
	   	memcpy(td_mt[j].sent_ip, node_ip_set[prty_node_id], ip_len);

<<<<<<< HEAD
=======
	   //printf("td->stripe_id=%d, td->updt_prty_nd_id[%d]=%d\n", td->stripe_id, j, prty_node_id);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	   pthread_create(&parix_updt_thread[j], NULL, send_updt_data_process, td_mt+j);
   
	   }
   
<<<<<<< HEAD
   // join the threads
=======
   //join the threads
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   for(j=0; j<num_updt_prty; j++)
	   pthread_join(parix_updt_thread[j], NULL);

   free(td_mt);
   free(parix_updt_thread);

}

<<<<<<< HEAD
/*
 * a thread receives ack 
 */
void* recv_ack_process(void* ptr){

=======

void* recv_ack_process(void* ptr){

	//printf("recv_ack_process works:\n");

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

<<<<<<< HEAD
    // receive data
=======
    //receive data
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    recv_len=0;
    while(recv_len < sizeof(ACK_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(ACK_DATA)-recv_len);
        recv_len+=read_size;

    }

<<<<<<< HEAD
    // copy the recv_buff to an ack structure
    ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
    memcpy(ack, recv_buff, sizeof(ACK_DATA));

    // judge the ack for different operations
=======
    //copy the data
    ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
    memcpy(ack, recv_buff, sizeof(ACK_DATA));

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	if(ack->op_type==PARIX_UPDT_CMLT)
		updt_cmlt_count[ack->updt_prty_id]++;

	else if(ack->op_type==PARIX_NEED_OLD_DT)
		need_old_dt_count[ack->updt_prty_id]++;

	else if(ack->op_type==LOG_CMLT)
		prty_log_cmplt_count[ack->updt_prty_id]++;

	else if(ack->op_type==CMMT_CMLT)
		commit_count[ack->updt_prty_id]++;

	else if(ack->op_type==MVMT_CMLT)
		mvmt_count[ack->data_chunk_id]++;

<<<<<<< HEAD
=======
	//printf("recv ack: td->stripe_id=%d, td->op_type=%d, td->from_ip=%s, td->next_ip=%s\n", td->stripe_id, td->op_type, td->from_ip, td->next_ip);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	free(ack);
	free(recv_buff);

	return NULL;

}


<<<<<<< HEAD
/*
 * receive data by using multiple threads
 */
void para_recv_ack(int stripe_id, int num_recv_chnks, int port_num){

=======
//receive data by using multiple threads
void para_recv_ack(int stripe_id, int num_recv_chnks, int port_num){

    //printf("para_recv_data starts:\n");

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    int i;
    int server_socket;
    int max_connctn;
    int index;
    int* connfd=malloc(sizeof(int)*num_recv_chnks);

    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    max_connctn=100;
    index=0;
	
    pthread_t recv_data_thread[data_chunks];

	if(num_recv_chnks > data_chunks){

		printf("ERROR: num_of_threads is not enough!\n");
		exit(1);

		}

<<<<<<< HEAD
    // initialize socket information
=======
    //initial socket information
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	server_socket=init_server_socket(port_num);

    if(listen(server_socket,max_connctn) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	RECV_PROCESS_DATA* rpd=(RECV_PROCESS_DATA *)malloc(sizeof(RECV_PROCESS_DATA)*num_recv_chnks);
	memset(rpd, 0, sizeof(RECV_PROCESS_DATA)*num_recv_chnks);


    while(1){

        connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
<<<<<<< HEAD

        //printf("Recv Ack from: ");
		//print_amazon_vm_info(inet_ntoa(sender_addr.sin_addr));
=======
        //printf("receive ack from %s\n",inet_ntoa(sender_addr.sin_addr));
        //printf("Recv Ack from: ");
		print_amazon_vm_info(inet_ntoa(sender_addr.sin_addr));
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

        rpd[index].connfd=connfd[index];
        rpd[index].recv_id=index;

		pthread_create(&recv_data_thread[index], NULL, recv_ack_process, (void *)(rpd+index));
<<<<<<< HEAD

		// if receiving enough data 
        index++;
        if(index>=num_recv_chnks)
			break;
        	

    }

    for(i=0; i<num_recv_chnks; i++)
        pthread_join(recv_data_thread[i], NULL);
    	
=======
		
        index++;

		//printf("index=%d, expect_num_recv_acks=%d\n", index, num_recv_chnks);

        if(index>=num_recv_chnks){
			//printf("index>=num_recv_chunks\n");
			break;
        	}

    }

    for(i=0; i<num_recv_chnks; i++){
		//printf("waiting join: i=%d, num_recv_chnks=%d\n", i, num_recv_chnks);
        pthread_join(recv_data_thread[i], NULL);
    	}

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	for(i=0; i<num_recv_chnks; i++)
		close(connfd[i]);

    free(rpd);
	free(connfd);
	close(server_socket);

}

<<<<<<< HEAD
/*
 * append a data chunk to the end of a file
 */
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void log_write(char* filename, TRANSMIT_DATA* td){

   int ret;
   char* tmp_buff=NULL;
   ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
<<<<<<< HEAD
   if(ret){
	   printf("ERROR: posix_memalign: %s\n", strerror(ret));
	   exit(1);
   	}

   memcpy(tmp_buff, td->buff, chunk_size);

   // move fd to the end of a file
=======
   if(ret)
	   printf("ERROR: posix_memalign: %s\n", strerror(ret));

   memcpy(tmp_buff, td->buff, chunk_size);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   int fd=open(filename, O_RDWR | O_CREAT, 0644);
   lseek(fd, 0, SEEK_END);
   
   ret=write(fd, tmp_buff, chunk_size);
   if(ret!=chunk_size){
	   perror("write_log_file_error!\n");
<<<<<<< HEAD
	   exit(1);
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   }

   free(tmp_buff);
   close(fd);

}


<<<<<<< HEAD
/*
 * This function is performed by the gateway, which forwards a updated data to a destination node
 */
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void gateway_forward_updt_data(TRANSMIT_DATA* td, char* sender_ip){

    /* record the source ip address */
	memcpy(td->from_ip, sender_ip, ip_len);
	send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);
	
}

<<<<<<< HEAD
/*
 * This function is performed by the gateway, which forwards an ack to a destination node
 */
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a

void gateway_forward_ack_info(ACK_DATA* ack){

	send_data(NULL, ack->next_ip, ack->port_num, ack, NULL, ACK_INFO);
	
}

<<<<<<< HEAD
/*
 * This function is performed by the gateway, which forwards a cmd data to a destination node
 */

=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void gateway_forward_cmd_data(CMD_DATA* cmd){

	send_data(NULL, cmd->next_ip, cmd->port_num, NULL, cmd, CMD_INFO);
	
}


<<<<<<< HEAD
/*
 * read a logged data chunk from a log file
 */
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void read_log_data(int local_chunk_id, char* log_data, char* filename){

    int i;
    int bucket_id;
    int log_order;
	int ret;
<<<<<<< HEAD
	
    //locate the log_order
    bucket_id=local_chunk_id%bucket_num;

=======

	//determine the log_order

/*
    //printf("num_updt_strps=%d\n", num_updt_strps);
	//printf("newest_chunk_log_data:\n");
    for(i=0; i<entry_per_bucket; i++){

        for(j=0; j<bucket_num; j++)
            printf("%d ", newest_chunk_log_order[j*entry_per_bucket*2+i*2]);

        printf("\n");
    }

	printf("newest_chunk_log_order:\n");
    for(i=0; i<entry_per_bucket; i++){

        for(j=0; j<bucket_num; j++)
            printf("%d ", newest_chunk_log_order[j*entry_per_bucket*2+i*2+1]);

        printf("\n");
    }
*/

    //read the old data from data file and new data from log file
    //locate the log_order
    bucket_id=local_chunk_id%bucket_num;

	//printf("bucket_id=%d, local_chunk_id=%d\n", bucket_id, local_chunk_id);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    for(i=0; i<entry_per_bucket; i++){

        if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]==local_chunk_id){
            log_order=newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i+1];
			break;
        	}
    }

	if(i>=entry_per_bucket){

		printf("ERROR: log not found!\n");
		exit(1);

		}

<<<<<<< HEAD
    // read the newest data from the log_file based on the log_order
    char* tmp_buff=NULL;
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
	if(ret){
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
		exit(1);
		}

    int fd=open(filename, O_RDONLY);
    lseek(fd, log_order*chunk_size, SEEK_SET);
	
=======
    //read the newest data from the log_file
    char* tmp_buff=NULL;
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
	if(ret)
		printf("ERROR: posix_memalign: %s\n", strerror(ret));

    int fd=open(filename, O_RDONLY);
    lseek(fd, log_order*chunk_size, SEEK_SET);
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    ret=read(fd, tmp_buff, chunk_size);
	if(ret<chunk_size){

		printf("read_log_data error!\n");
		exit(1);
		
		}
	
    close(fd);

	memcpy(log_data, tmp_buff, sizeof(char)*chunk_size);

	free(tmp_buff);

}


<<<<<<< HEAD
/*
 * eivct a data chunk out of a log file
 */
=======
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void evict_log_dt(int* log_table, int logcal_data_id){

	int i; 
	int bucket_id;

	bucket_id=logcal_data_id%bucket_num;
	
    for(i=0; i<entry_per_bucket; i++){

        // if find the given_chunk_id, udpate its log order
        if(log_table[bucket_id*entry_per_bucket*2+2*i]==logcal_data_id)
            break;
		
    }

    //reset the bucket table after committing the logged data
    log_table[bucket_id*entry_per_bucket*2+2*i]=-1;
	log_table[bucket_id*entry_per_bucket*2+2*i+1]=-1;

}

<<<<<<< HEAD
/*
 * This function is performed by the client to receive the metadata from the metadata server
 */ 
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void recv_metadata(META_INFO* metadata, int port_num){

    int server_socket;
	int connfd;
	int recv_len, read_size;

	char* recv_buff=(char*)malloc(sizeof(META_INFO));
	
	server_socket=init_server_socket(port_num);

<<<<<<< HEAD
    // init the sender info
=======
    //init the sender info
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    if(listen(server_socket, 20) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	while(1){

		connfd=accept(server_socket, (struct sockaddr*)&sender_addr, &length);
		if(connfd<0){
<<<<<<< HEAD
			perror("recv_metadata fails\n");
			exit(1);
=======

			perror("recv_metadata fails\n");
			exit(1);

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
			}

		recv_len=0;
		read_size=0;
		while(recv_len < sizeof(META_INFO)){

			read_size=read(connfd, recv_buff+recv_len, sizeof(META_INFO)-recv_len);
			recv_len += read_size;

			}

<<<<<<< HEAD
		// copy the data in buff 
		memcpy(metadata, recv_buff, sizeof(META_INFO));

		// close the connection 
=======
		//copy the data in buff 
		memcpy(metadata, recv_buff, sizeof(META_INFO));

		//close the connection 
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
		close(connfd);

		break;
		
		}

<<<<<<< HEAD
=======

>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
	free(recv_buff);
	close(server_socket);

}



<<<<<<< HEAD
/* 
 * this function is executed by the client to connect the metadata server for data update
 */
=======
/* this function is executed by the client to connect the metadata server for data update */
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
void connect_metaserv(int chunk_id, META_INFO* metadata){

   UPDT_REQ_DATA* req=(UPDT_REQ_DATA*)malloc(sizeof(UPDT_REQ_DATA));

   req->op_type=UPDT_REQ;
   req->local_chunk_id=chunk_id;

<<<<<<< HEAD
   // send the req to metadata server
   send_req(req, mt_svr_ip, UPDT_PORT, NULL, REQ_INFO);

   // recv the metadata information
=======
   //send the req to metadata server
   send_req(req, mt_svr_ip, UPDT_PORT, NULL, REQ_INFO);

   //recv the metadata information
>>>>>>> 03c92af8f9ce1f366a9a26c128f98adb9fcdf95a
   recv_metadata(metadata, UPDT_PORT);

   free(req);

}

