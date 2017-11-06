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

#include "config.h"

char* node_ip_set[6]={"192.168.0.15", "192.168.0.16", "192.168.0.17", "192.168.0.18", "192.168.0.19", "192.168.0.20"};
char* mt_svr_ip="192.168.0.1";
int   nodes_in_racks[3]={2,2,2};


void print_array(int row, int col, int *array){

  int i,j; 

  for(i=0; i<row; i++){

	for(j=0; j<col; j++)
		printf("%d ", array[i*col+j]);

	printf("\n");

  	}
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
	int index;

	for(i=0; i<n; i++){

		if(array[i]>ret){
			ret=array[i];
			index=i;
			}

		}

	return index;
}




//this function is to read the mapping information from disk and keep it in the memory 
void read_chunk_map(char* map_file){

	int j;
	char strline[strlen];
	int index;
	int stripe_id;

	FILE *fd=fopen(map_file, "r");
	if(fd==NULL)
		printf("open_file error!\n");

    stripe_id=0;
	while(fgets(strline, strlen, fd)!=NULL){

	  //printf("%s",strline);
	  index=0;

	  for(j=0; j<strlen; j++){

		if(strline[j]=='\0')
			break;

		if(strline[j]>='0' && strline[j]<='9'){

			global_chunk_map[stripe_id*num_chunks_in_stripe+index]=strline[j]-'0';
			//printf("global_chunk_map[%d][%d]=%d\n", stripe_id, index, global_chunk_map[stripe_id*num_chunks_in_stripe+index]);
			index++;
			}
		}
	  
	  stripe_id++;
	}

	fclose(fd);

	//print_array(stripe_num, num_chunks_in_stripe, global_chunk_map);
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

int init_client_socket(){

    //set client_addr info
    struct sockaddr_in client_addr;
    bzero(&client_addr,sizeof(client_addr));
    client_addr.sin_family = AF_INET;
    client_addr.sin_addr.s_addr = htons(INADDR_ANY);
    client_addr.sin_port = htons(0);

    //create client socket
    int client_socket = socket(AF_INET,SOCK_STREAM,0);
    int on=1;
    int ret; 

    if( client_socket < 0)
    {
        perror("Create Socket Failed!\n");
        //exit(1);
    }

	ret=setsockopt(client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    //combine client_socket with client_addr
    if(bind(client_socket,(struct sockaddr*)&client_addr,sizeof(client_addr)))
    {
        perror("Client Bind Port Failed!\n");
        exit(1);
    }

    return client_socket;

}



// this function is executed by the internal node, which send the aggregated data delta to the parity node for final encoding
void send_data(TRANSMIT_DATA *td, char *server_ip, int port_num){


    int sent_len;
    int ret;

	//printf("send_port=%d\n", port_num);

    char* send_buff=malloc(sizeof(TRANSMIT_DATA));

	int client_socket=init_client_socket();

    //set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    if(inet_aton(server_ip,&server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
		printf("Server IP is %s\n", server_ip);
        exit(1);
    }
    server_addr.sin_port = htons(port_num);
    socklen_t server_addr_length = sizeof(server_addr);

    while(connect(client_socket,(struct sockaddr*)&server_addr, server_addr_length) < 0);
    //printf("connect success!\n");

    memcpy(send_buff, td, sizeof(TRANSMIT_DATA));
    sent_len=0;
    while(sent_len < sizeof(TRANSMIT_DATA)){
        ret=write(client_socket, send_buff+sent_len, sizeof(TRANSMIT_DATA));
        sent_len+=ret;
    }

    free(send_buff);
	close(client_socket);
    //printf("send completes!\n");
}


// this function sends updated data to servers
void send_updt_data(TRANSMIT_DATA* td, char* server_ip, int port_num){

    int ret;
    int sent_len;

    int client_socket=init_client_socket();
	
    //set server_addr info
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;

    if(inet_aton(server_ip,&server_addr.sin_addr) == 0)
    {
        printf("Server IP Address Error!\n");
        exit(1);
    }
    server_addr.sin_port = htons(port_num);
    socklen_t server_addr_length = sizeof(server_addr);

    //connect server
    while(connect(client_socket,(struct sockaddr*)&server_addr, server_addr_length) < 0);
    //printf("connect success!\n");

    //init sent_buff
    char *sent_buff=malloc(sizeof(TRANSMIT_DATA));
    memcpy(sent_buff,td,sizeof(TRANSMIT_DATA));

    sent_len=0;
    while(sent_len < sizeof(TRANSMIT_DATA)){
        ret=write(client_socket, sent_buff+sent_len, sizeof(TRANSMIT_DATA));
        sent_len+=ret;
    }

    free(sent_buff);
	close(client_socket);

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
        printf("Create Socket Failed!");
        exit(1);
    }

    ret=setsockopt(server_socket,SOL_SOCKET,SO_REUSEADDR,(char *)&opt,sizeof(opt)); //set the portnum reusable
	if(ret!=0){

		perror("setsockopt error!\n");
		exit(1);

		}

    if(bind(server_socket,(struct sockaddr*)&server_addr,sizeof(server_addr))){
        printf("Server Bind Port : %d Failed!", port_num);
        exit(1);
    }

	return server_socket;

}


//we use module as the hash algorithm
void update_loged_chunks(int given_chunk_id){

    int bucket_id;
    int i,j;

    bucket_id=given_chunk_id%bucket_num;

    // if the bucket is full
    if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*(entry_per_bucket-1)]>0){

        printf("Error! bucket_%d is full!\n", bucket_id);
        exit(0);

    }

    //scan the entries in that bucket
    for(i=0; i<entry_per_bucket; i++){

        // if find the given_chunk_id, udpate its log order
        if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]==given_chunk_id)
            break;

        // if reach the initialized ones
        if(newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]==-1)
            break;
    }

    // record the given_chunk_id
    newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i]=given_chunk_id;
    newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i+1]=new_log_chunk_cnt;

/*
    printf("updated_bucket_record:\n");
    for(i=0; i<entry_per_bucket; i++){

        for(j=0; j<bucket_num; j++)
            printf("%d ", newest_chunk_log_order[j*entry_per_bucket*2+i*2]);

        printf("\n");
    }

    printf("\n");

    printf("log_cnt:\n");
    for(i=0; i<entry_per_bucket; i++){

        for(j=0; j<bucket_num; j++)
            printf("%d ", newest_chunk_log_order[j*entry_per_bucket*2+i*2+1]);

        printf("\n");
    }
*/
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






//generate a random string with the size of len
void gene_radm_buff(char* buff, int len){

    int i;


    static const char alphanum[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    srand((unsigned int)time(0));
    for(i=0; i<chunk_size; i++)
        buff[i]=alphanum[rand()%(sizeof(alphanum)-1)];

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


//given a chunk to be updated, we locate the node that stores the chunk
int get_dest_node_id(int updt_chunk_id){


    int stripe_id;
    int id;

    int dest_node_id;

    stripe_id = updt_chunk_id/data_chunks;
    id=updt_chunk_id%data_chunks;

    dest_node_id=global_chunk_map[stripe_id*num_chunks_in_stripe+id];

    return dest_node_id;
}


char* GetLocalIp()
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

    return inet_ntoa(sin.sin_addr);

}


//this function gets the host_ip and returns the local_node_id in the node_ip_set
int get_local_node_id(){

    char* local_ip;
    int i;
    int ret;

    local_ip=GetLocalIp();
    //printf("server_ip=%s\n",local_ip);

    //locate the rack
    for(i=0; i<total_nodes_num; i++){

        if((ret=strcmp(node_ip_set[i],local_ip))==0)
            break;

    }

    return i;

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
                    exit(0);

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
void aggregate_data(char* data_delta, int num_recv_chnks){

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

        bitwiseXor(res, addrA, intnl_recv_data+i*chunk_size*sizeof(char), chunk_size*sizeof(char));
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

    fd=open("data_file", O_RDWR);
    lseek(fd, stored_index*chunk_size, SEEK_SET);
    ret=write(fd, new_data, chunk_size);
    if(ret!=chunk_size)
        printf("read data error!\n");

    close(fd);
}

void listen_ack(TRANSMIT_DATA* cmt_ntf_td, char* recv_buff, int stripe_id, int updt_dt_id, int updt_prty_id, int port_num, int op_type){

   //printf("listen_ack:\n");

   int recv_len; 
   int read_size;
   int ntf_connfd;
   int ntf_socket;


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
   //printf("get connection:\n");
   //printf("receive connection from %s\n", inet_ntoa(sender_addr.sin_addr));

   recv_len=0; 
   while(recv_len<sizeof(TRANSMIT_DATA)){

	   read_size = read(ntf_connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA));
	   recv_len+=read_size;

	   //printf("recv_len=%d, expected_size=%lu\n", recv_len, sizeof(TRANSMIT_DATA));

	}

    memcpy(cmt_ntf_td, recv_buff, sizeof(TRANSMIT_DATA));

	//printf("cmt_ntf_td->op_type=%d\n",cmt_ntf_td->op_type);
	//printf("cmt_ntf_td->stripe_id=%d\n", cmt_ntf_td->stripe_id);
	//printf("cmt_ntf_td->data_chunk_id=%d\n", cmt_ntf_td->data_chunk_id);
	//printf("cmt_ntf_td->updt_prty_id=%d\n", cmt_ntf_td->updt_prty_id);

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
	close(ntf_socket);
	close(ntf_connfd);

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

	TRANSMIT_DATA* ntf_ack_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
			 
	ntf_ack_td->op_type=op_type;
	ntf_ack_td->stripe_id=stripe_id;
	ntf_ack_td->role=PARITY;
	ntf_ack_td->updt_prty_id=prty_id;
			 
	ntf_ack_td->data_chunk_id=dt_id;
	ntf_ack_td->num_recv_chks_itn=-1;
	ntf_ack_td->num_recv_chks_prt=-1;
	memset(ntf_ack_td->next_ip, '0', 50);
	memset(ntf_ack_td->buff, '0', chunk_size*sizeof(char)); 
	send_data(ntf_ack_td, destined_ip, port_num);
	
	free(ntf_ack_td);

}




//given a node_id and a global_chunk_id, return the store index of that chunk on that node 
int locate_store_index(int node_id, int global_chunk_id){

    int start_index, end_index;
	int mid_index;
	int store_index;
	
	//locate the stored order of given_chunk_id by using binary search
    start_index=0;
    end_index=num_store_chunks[node_id]-1;

	//printf("chunk_store_order:\n");
	//print_array(num_store_chunks, 1, chunk_store_order);

    //printf("num_store_chunks=%d, global_chunk_id=%d\n",num_store_chunks[node_id], global_chunk_id);

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

		//printf("start_chunk=%d, end_chunk=%d\n", chunk_store_order[start_index], chunk_store_order[end_index]);

		if(start_index>=end_index){
			printf("search_global_chunk_id error!\n");
			exit(1);
			}
		
    }

	return store_index;


}



//receive the data and copy the buffer to a desinated area
void* data_mvmnt_process(void* ptr){

    //printf("recv_data_process works:\n");

    RECV_PROCESS_DATA rpd=*(RECV_PROCESS_DATA *)ptr;

    int recv_len;
    int read_size;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));

    //receive data
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-sizeof(int));
        recv_len+=read_size;

    }

    //copy the data
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

	if(td->role==IN_CHNK)
		memcpy(in_chunk, td->buff, chunk_size);

	else if(td->role==OUT_CHNK)
		memcpy(out_chunk, td->buff, chunk_size);

    close(rpd.connfd);
    free(td);
    free(recv_buff);

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

        read_size=read(rpd.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-sizeof(int));
        recv_len+=read_size;

    }

    //copy the data
    TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));
    memcpy(intnl_recv_data+chunk_size*sizeof(char)*rpd.recv_id, td->buff, chunk_size*sizeof(char));

    close(rpd.connfd);
    free(td);
    free(recv_buff);

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
    int* connfd=malloc(sizeof(int)*num_recv_chnks);

    max_connctn=100;
    index=0;
	
    pthread_t recv_data_thread[data_chunks];

    //initial socket information
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htons(INADDR_ANY);
    server_addr.sin_port = htons(port_num);

	//printf("recv_port_num=%d\n", port_num);

    //init the sender info
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    server_socket = socket(PF_INET,SOCK_STREAM,0);
    if( server_socket < 0){
        printf("Create Socket Failed!");
        exit(1);
    }

    if(bind(server_socket,(struct sockaddr*)&server_addr,sizeof(server_addr))){
		perror("Server Bind Port : %d Failed!");
        exit(1);
    }

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
    	}

	for(i=0; i<num_recv_chnks; i++)
		close(connfd[i]);

    free(rpd);
	//close the sockets 
	close(server_socket);
	//printf("recv_completes\n");

}


