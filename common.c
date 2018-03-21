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
char* node_ip_set[total_nodes_num]={"13.125.219.94", "13.125.213.113", "13.125.241.211", "13.125.205.78", "13.125.221.99", //seoul nodes
									"13.250.2.168", "54.255.236.120", "13.250.109.234", "54.169.201.194", "54.255.207.255", //sgp nodes
									"13.211.138.59", "54.252.226.100", "13.211.234.168", "54.252.191.111", "13.211.95.45", //sydney nodes
									"54.168.99.38", "13.231.125.11", "13.231.255.124", "13.231.109.236", "13.113.190.179"}; //tokyo

/* it records the inner ip address of amazon vms read from eth0 */
char* inner_ip_set[total_nodes_num]={"172.31.22.65", "172.31.25.109", "172.31.28.40", "172.31.31.102", "172.31.21.104", //seoul region
"172.31.12.140", "172.31.10.209", "172.31.12.191", "172.31.13.62", "172.31.6.226", //sgp region
"172.31.31.128", "172.31.19.172", "172.31.29.105", "172.31.27.60", "172.31.22.26", //sydney region
"172.31.24.91", "172.31.21.42", "172.31.29.227", "172.31.25.150", "172.31.17.236"}; //tokyo region

char* mt_svr_ip="13.250.102.147";
char* client_ip="13.229.232.195";
char* gateway_ip="13.229.232.195";

/* we currently consider all the regions have the same number of nodes */
int   nodes_in_racks[rack_num]={node_num_per_rack, node_num_per_rack, node_num_per_rack, node_num_per_rack};

char* region_name[rack_num]={"Seoul", "Singapore", "Sydney", "Tokyo"};


// a hash function to check the integrity of received data
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


/* this function returns the rack_id of a given node_id */
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


/* print the region info and node info */
void print_amazon_vm_info(char* node_ip){

	int node_id=get_node_id(node_ip);
	int rack_id=get_rack_id(node_id);
	int base=node_num_per_rack;
	
	printf("Region-%s, Node-%d\n", region_name[rack_id], node_id%base);

}

/* print all the elements in an array */
void print_array(int row, int col, int *array){

  int i,j; 

  for(i=0; i<row; i++){

	for(j=0; j<col; j++)
		printf("%d ", array[i*col+j]);

	printf("\n");

  	}
}

/* calculate the sum of all the elements in an array */
int sum_array(int num, int* arr){

	int count=0;
	int i;

	for(i=0; i<num; i++)
		count+=arr[i];

	return count;
}



int find_max_array(int* array, int n){

	int i;
	int ret=-1;

	for(i=0; i<n; i++){

		if(array[i]>ret)
			ret=array[i];

		}

	return ret;
}

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


	fclose(fd);

}




/* this function reads the ip address from the nic eth0 */
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


//this function gets the host_ip and returns the local_node_id in the node_ip_set
int get_local_node_id(){

    char local_ip[ip_len];
    int i;
    int ret;

    GetLocalIp(local_ip);
    //printf("server_ip=%s\n",local_ip);

	if(if_gateway_open==1){

       //locate the node id
       for(i=0; i<total_nodes_num; i++){

        if((ret=strcmp(node_ip_set[i],local_ip))==0)
            break;

		return i;

       }
	 }

	else {

		for(i=0; i<total_nodes_num; i++)
			if(strcmp(local_ip, inner_ip_set[i])==0)
				break;

		return i;

		}

}



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
        //exit(1);
    }

	setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    return client_socket;

}



// this function is executed by the internal node, which send the aggregated data delta to the parity node for final encoding
void send_data(TRANSMIT_DATA *td, char *server_ip, int port_num, ACK_DATA* ack, CMD_DATA* cmd, int send_data_type){

    int sent_len;
    int ret;
	int client_socket;
	int data_size;

	//printf("send_port=%d\n", port_num);

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
    //printf("connect success!\n");
    
    sent_len=0;
    while(sent_len < data_size){
        ret=write(client_socket, send_buff+sent_len, data_size-sent_len);
        sent_len+=ret;
    }

    free(send_buff);
	ret=close(client_socket);
	if(ret==-1)
		perror("close_send_data_socket error!\n");
	
    //printf("send completes!\n");
}


// this function is executed by the internal node, which send the aggregated data delta to the parity node for final encoding
void send_req(UPDT_REQ_DATA* req, char* server_ip, int port_num, META_INFO* metadata, int info_type){

    int sent_len;
    int ret;
	int client_socket;
	int data_size;

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


/* this function transforms a char type to an integer type */
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



//generate a random string with the size of len
void gene_radm_buff(char* buff, int len){

    int i;
	
    char alphanum[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for(i=0; i<chunk_size; i++)
        buff[i]=alphanum[i%(sizeof(alphanum)-1)];

}

//count the number of non-negatives in an array
int count_non_ngtv(int* arr, int len){


    int i;
    int count;

    count=0;
    for(i=0; i<len; i++)
        if(arr[i]>=0)
            count++;

    return count;
}


//given the nodes_in_racks, the set of ip addresses, this function gets the rack_id that the node resides in
int get_local_chunk_id(int stripe_id){

    int local_node_id;
    int local_data_chunk_id;
    int i;

    local_node_id=get_local_node_id();

	//printf("local_node_id=%d\n", local_node_id);

    //get the local_chunk_id
    for(i=0; i<data_chunks; i++){

        if(global_chunk_map[stripe_id*num_chunks_in_stripe+i]==local_node_id)
            break;

    }

	if(i==data_chunks){

		printf("does not find the chunk\n");
		exit(1);
		
		}

    local_data_chunk_id=stripe_id*data_chunks+i; //we promise that the chunk_id exists, as we always select a node that has that chunk for partial encoding

    //printf("local_data_chunk_id=%d\n",local_data_chunk_id);

    return local_data_chunk_id;

}




//given the chunk_map, this function outputs the store order of each chunk on every node
void get_chunk_store_order(){

    int i,j;
	int k;

	for(k=0; k<total_nodes_num; k++){
		
		num_store_chunks[k]=0;
		
        for(i=0; i<stripe_num; i++){
          for(j=0; j<num_chunks_in_stripe; j++){

              if(global_chunk_map[i*num_chunks_in_stripe+j]==k){

                chunk_store_order[k*max_num_store_chunks+num_store_chunks[k]]=i*num_chunks_in_stripe+j; // use "GLOBAL" order to represent store order of ALL chunks
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

// this function is to calculate the data delta of two regions
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


//for each receive data, perform XOR operations to them
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

        //printf("res_addr=%p, addrA=%p\n", res, addrA);
        bitwiseXor(res, addrA, ped+i*chunk_size*sizeof(char), chunk_size*sizeof(char));
	    //printf("res=%x, addrA_addr=%x\n", tmp, addrA);
        tmp=addrA;
        addrA=res;
        res=tmp;
    }
	
    memcpy(data_delta, addrA, chunk_size);

	//printf("out aggregated_data:\n");

}

void flush_new_data(int stripe_id, char* new_data, int global_chunk_id, int stored_index){

    int fd;
    int ret;
    char* tmp_buff=NULL;
	
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
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

    close(fd);
	free(tmp_buff);

    gettimeofday(&ed_tm, NULL);
    //printf("flush_write_time=%lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}

void listen_ack(ACK_DATA* ack, char* recv_buff, int stripe_id, int updt_dt_id, int updt_prty_id, int port_num, int op_type){

   //printf("listen_ack:\n");

   int recv_len; 
   int read_size;
   int ntf_connfd;
   int ntf_socket;
   int ret;


   //initial socket information
   ntf_socket=init_server_socket(port_num);
   
   //init the sender info
   struct sockaddr_in sender_addr;
   socklen_t length=sizeof(sender_addr);
   
   if(listen(ntf_socket,100) == -1){
	   printf("Failed to listen.\n");
	   exit(1);
   }

   ntf_connfd = accept(ntf_socket, (struct sockaddr*)&sender_addr, &length);
   //printf("receive ack from: ");
   //print_amazon_vm_info(inet_ntoa(sender_addr.sin_addr));

   recv_len=0; 
   while(recv_len<sizeof(ACK_DATA)){

	   read_size = read(ntf_connfd, recv_buff+recv_len, sizeof(ACK_DATA)-recv_len);
	   recv_len+=read_size;

	   //printf("recv_len=%d, expected_size=%lu\n", recv_len, sizeof(TRANSMIT_DATA));

	}

	close(ntf_connfd);
	ret=close(ntf_socket);
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
int obtain_encoding_coeff(int given_chunk_id, int prtyid_to_update){


    int index;

    index=given_chunk_id%data_chunks;

	//printf("prty_to_update=%d, index=%d\n", prtyid_to_update, index);

    return encoding_matrix[prtyid_to_update*data_chunks+index];
}


void encode_data(char* data, char* pse_coded_data, int chunk_id, int updt_prty){

	//printf("encode_data works:\n");

    int ecd_cef=obtain_encoding_coeff(chunk_id,updt_prty);
	//printf("encod_marix[%d][%d]=encod_coeff=%d\n", updt_prty, chunk_id%data_chunks, ecd_cef);
    galois_w08_region_multiply(data, ecd_cef, chunk_size, pse_coded_data, 0);

}

//send ack to the destined node
void send_ack(int stripe_id, int dt_id, int prty_id, char* destined_ip, int port_num, int op_type){

    int node_id; 
	int des_node_id;
	int rack_id;
	int des_rack_id;

	//init the sent info
	
	//init ack_data
	ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));

	ack->send_size=sizeof(ACK_DATA);
	ack->op_type=op_type;
	ack->stripe_id=stripe_id;
	ack->data_chunk_id=dt_id;
	ack->updt_prty_id=prty_id;
	ack->port_num=port_num;
	memcpy(ack->next_ip, destined_ip, ip_len);
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
	if(strcmp(destined_ip, client_ip)==0){

		memcpy(ack->next_ip, client_ip, ip_len);

		if(if_gateway_open==1)
			send_data(NULL, gateway_ip, SERVER_PORT, ack, NULL, ACK_INFO);

		else 
			send_data(NULL, ack->next_ip, port_num, ack, NULL, ACK_INFO);
		
		return;

		}

    //determine the source node and the destination node and their racks
	node_id=get_local_node_id();
	rack_id=get_rack_id(node_id);

	des_node_id=get_node_id(destined_ip);
	des_rack_id=get_rack_id(des_node_id);

	//printf("send ack: node_id=%d, rack_id=%d, des_node_id=%d, des_rack_id=%d\n", node_id, rack_id, des_node_id, des_rack_id);

    //if the gateway is opened and the two nodes are in different racks, then forward the data to the gateway first
	if((if_gateway_open==1) && (des_rack_id!=rack_id)){

		memcpy(ack->next_ip, destined_ip, ip_len);
		send_data(NULL, gateway_ip, SERVER_PORT, ack, NULL, ACK_INFO);

		}

	else 
		send_data(NULL, ack->next_ip, port_num, ack, NULL, ACK_INFO);
		
	
	free(ack);

	//printf("send_ack finishes\n");

}


//given a node_id and a global_chunk_id, return the store index of that chunk on that node 
int locate_store_index(int node_id, int global_chunk_id){

    int start_index, end_index;
	int mid_index;
	int store_index;
	
	//locate the stored order of given_chunk_id by using binary search
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

		//printf("start_chunk=%d, end_chunk=%d\n", chunk_store_order[node_id*max_num_store_chunks+start_index], chunk_store_order[node_id*max_num_store_chunks+end_index]);
		//printf("global_chunk_id=%d\n", global_chunk_id);

		if(start_index>=end_index){
			printf("search_global_chunk_id error!\n");
			exit(1);
			}
		
    }

	return store_index;


}



//receive the data and copy the buffer to a desinated area
void* data_mvmnt_process(void* ptr){

    //printf("data_mvmnt_process works:\n");

    RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

    //receive data
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-recv_len);
        recv_len+=read_size;

    }

    //copy the data
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


//receive the data and copy the buffer to a desinated area
void* recv_data_process(void* ptr){

    //printf("recv_data_process works:\n");

    RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

    //receive data
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-recv_len);
        recv_len+=read_size;

    }

    //copy the data
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

	//copy the delta and record the data id
    memcpy(intnl_recv_data+chunk_size*sizeof(char)*rpd.recv_id, td->buff, chunk_size*sizeof(char));
	intnl_recv_data_id[td->data_chunk_id]=1;

    free(td);
    free(recv_buff);

	return NULL;

}


//receive data by using multiple threads
//if @flag_tag=1, then it performs recv_data_process
//if @flag_tag=2, then it performs data_mvmnt_process
void para_recv_data(int stripe_id, int num_recv_chnks, int port_num, int flag_tag){


    //printf("para_recv_data starts:\n");

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

    //init the sender info
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
        //printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));

        rpd[index].connfd=connfd[index];
        rpd[index].recv_id=index;

		if(flag_tag==1)
            pthread_create(&recv_data_thread[index], NULL, recv_data_process, (void *)(rpd+index));

		else 
			pthread_create(&recv_data_thread[index], NULL, data_mvmnt_process, (void *)(rpd+index));
        index++;

        if(index>=num_recv_chnks){
			//printf("index>=num_recv_chunks\n");
			break;
        	}

    }

    for(i=0; i<num_recv_chnks; i++){
		//printf("waiting join: i=%d, num_recv_chnks=%d\n", i, num_recv_chnks);
        pthread_join(recv_data_thread[i], NULL);
		close(connfd[i]);
    	}

    free(rpd);
	free(connfd);
	
	ret=close(server_socket);
	if(ret==-1)
	   perror("close_para_recv_data error!\n");
	
	//printf("recv_completes\n");

}


void read_old_data(char* read_buff, int store_index){

    int ret;
    int fd;
	char* tmp_buff=NULL;

    //read the old data from data_file
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
	if(ret)
		printf("ERROR: posix_memalign: %s\n", strerror(ret));
		

    fd=open("data_file", O_RDONLY);
    lseek(fd, store_index*chunk_size, SEEK_SET);
    ret=read(fd, tmp_buff, chunk_size);
    if(ret!=chunk_size){
        printf("read data error!\n");
		exit(1);
    	}

    //printf("read old data succeeds\n");
	memcpy(read_buff, tmp_buff, chunk_size);

    close(fd);
	free(tmp_buff);

}

//in-place write the new data
void write_new_data(char* write_buff, int store_index){

   int fd; 
   int ret; 
   char* tmp_buff=NULL;

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
   
   close(fd);
   free(tmp_buff);

   gettimeofday(&ed_tm, NULL);
   //printf("write_new_data_time=%lf\n", ed_tm.tv_sec-bg_tm.tv_sec+(ed_tm.tv_usec-bg_tm.tv_usec)*1.0/1000000);

}


void* send_updt_data_process(void* ptr){

   TRANSMIT_DATA td = *(TRANSMIT_DATA *)ptr;

   printf("send_updt_data to: ");
   print_amazon_vm_info(td.sent_ip);

   send_data((TRANSMIT_DATA *)ptr, td.sent_ip, td.port_num, NULL, NULL, UPDT_DATA);

   return NULL;
}


//send the new data to the num_updt_prty parity chunks
void para_send_dt_prty(TRANSMIT_DATA* td, int op_type, int num_updt_prty, int port_num){

   int j; 
   int prty_node_id;
   int prty_rack_id;
   int node_id;
   int rack_id;

   //brodcast the new data to m parity chunks
   TRANSMIT_DATA* td_mt=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*num_updt_prty);

   node_id=get_local_node_id();
   rack_id=get_rack_id(node_id);
   
   pthread_t* parix_updt_thread=(pthread_t*)malloc(sizeof(pthread_t)*num_updt_prty);
   memset(parix_updt_thread, 0, sizeof(pthread_t)*num_updt_prty);

   for(j=0; j<num_updt_prty; j++){
   
	   //init td structure
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

	   //printf("prty_node_id=%d, prty_ip=%s\n", prty_node_id, node_ip_set[prty_node_id]);

	   if((if_gateway_open==1) && (rack_id!=prty_rack_id)){
		  memcpy(td_mt[j].sent_ip, gateway_ip, ip_len);
		  memcpy(td_mt[j].next_ip, node_ip_set[prty_node_id], ip_len);
	   	}
	   
	   else 
	   	memcpy(td_mt[j].sent_ip, node_ip_set[prty_node_id], ip_len);

	   //printf("td->stripe_id=%d, td->updt_prty_nd_id[%d]=%d\n", td->stripe_id, j, prty_node_id);
	   pthread_create(&parix_updt_thread[j], NULL, send_updt_data_process, td_mt+j);
   
	   }
   
   //join the threads
   for(j=0; j<num_updt_prty; j++)
	   pthread_join(parix_updt_thread[j], NULL);

   free(td_mt);
   free(parix_updt_thread);

}


void* recv_ack_process(void* ptr){

	//printf("recv_ack_process works:\n");

	RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

    //receive data
    recv_len=0;
    while(recv_len < sizeof(ACK_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(ACK_DATA)-recv_len);
        recv_len+=read_size;

    }

    //copy the data
    ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
    memcpy(ack, recv_buff, sizeof(ACK_DATA));

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

	//printf("recv ack: td->stripe_id=%d, td->op_type=%d, td->from_ip=%s, td->next_ip=%s\n", td->stripe_id, td->op_type, td->from_ip, td->next_ip);

	free(ack);
	free(recv_buff);

	return NULL;

}


//receive data by using multiple threads
void para_recv_ack(int stripe_id, int num_recv_chnks, int port_num){

    //printf("para_recv_data starts:\n");

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

    //initial socket information
	server_socket=init_server_socket(port_num);

    if(listen(server_socket,max_connctn) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	RECV_PROCESS_DATA* rpd=(RECV_PROCESS_DATA *)malloc(sizeof(RECV_PROCESS_DATA)*num_recv_chnks);
	memset(rpd, 0, sizeof(RECV_PROCESS_DATA)*num_recv_chnks);


    while(1){

        connfd[index] = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
        //printf("receive ack from %s\n",inet_ntoa(sender_addr.sin_addr));
        //printf("Recv Ack from: ");
		print_amazon_vm_info(inet_ntoa(sender_addr.sin_addr));

        rpd[index].connfd=connfd[index];
        rpd[index].recv_id=index;

		pthread_create(&recv_data_thread[index], NULL, recv_ack_process, (void *)(rpd+index));
		
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

	for(i=0; i<num_recv_chnks; i++)
		close(connfd[i]);

    free(rpd);
	free(connfd);
	close(server_socket);

}

void log_write(char* filename, TRANSMIT_DATA* td){

   int ret;
   char* tmp_buff=NULL;
   ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
   if(ret)
	   printf("ERROR: posix_memalign: %s\n", strerror(ret));

   memcpy(tmp_buff, td->buff, chunk_size);

   int fd=open(filename, O_RDWR | O_CREAT, 0644);
   lseek(fd, 0, SEEK_END);
   
   ret=write(fd, tmp_buff, chunk_size);
   if(ret!=chunk_size){
	   perror("write_log_file_error!\n");

   }

   free(tmp_buff);
   close(fd);

}


void gateway_forward_updt_data(TRANSMIT_DATA* td, char* sender_ip){

    /* record the source ip address */
	memcpy(td->from_ip, sender_ip, ip_len);
	send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);
	
}


void gateway_forward_ack_info(ACK_DATA* ack){

	send_data(NULL, ack->next_ip, ack->port_num, ack, NULL, ACK_INFO);
	
}

void gateway_forward_cmd_data(CMD_DATA* cmd){

	send_data(NULL, cmd->next_ip, cmd->port_num, NULL, cmd, CMD_INFO);
	
}



void read_log_data(int local_chunk_id, char* log_data, char* filename){

    int i;
    int bucket_id;
    int log_order;
	int ret;

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

    //read the newest data from the log_file
    char* tmp_buff=NULL;
	ret=posix_memalign((void **)&tmp_buff, getpagesize(), chunk_size);
	if(ret)
		printf("ERROR: posix_memalign: %s\n", strerror(ret));

    int fd=open(filename, O_RDONLY);
    lseek(fd, log_order*chunk_size, SEEK_SET);
    ret=read(fd, tmp_buff, chunk_size);
	if(ret<chunk_size){

		printf("read_log_data error!\n");
		exit(1);
		
		}
	
    close(fd);

	memcpy(log_data, tmp_buff, sizeof(char)*chunk_size);

	free(tmp_buff);

}


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


void recv_metadata(META_INFO* metadata, int port_num){

    int server_socket;
	int connfd;
	int recv_len, read_size;

	char* recv_buff=(char*)malloc(sizeof(META_INFO));
	
	server_socket=init_server_socket(port_num);

    //init the sender info
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    if(listen(server_socket, 20) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

	while(1){

		connfd=accept(server_socket, (struct sockaddr*)&sender_addr, &length);
		if(connfd<0){

			perror("recv_metadata fails\n");
			exit(1);

			}

		recv_len=0;
		read_size=0;
		while(recv_len < sizeof(META_INFO)){

			read_size=read(connfd, recv_buff+recv_len, sizeof(META_INFO)-recv_len);
			recv_len += read_size;

			}

		//copy the data in buff 
		memcpy(metadata, recv_buff, sizeof(META_INFO));

		//close the connection 
		close(connfd);

		break;
		
		}


	free(recv_buff);
	close(server_socket);

}



/* this function is executed by the client to connect the metadata server for data update */
void connect_metaserv(int chunk_id, META_INFO* metadata){

   UPDT_REQ_DATA* req=(UPDT_REQ_DATA*)malloc(sizeof(UPDT_REQ_DATA));

   req->op_type=UPDT_REQ;
   req->local_chunk_id=chunk_id;

   //send the req to metadata server
   send_req(req, mt_svr_ip, UPDT_PORT, NULL, REQ_INFO);

   //recv the metadata information
   recv_metadata(metadata, UPDT_PORT);

   free(req);

}

