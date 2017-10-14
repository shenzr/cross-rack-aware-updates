#define _GNU_SOURCE 

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <pthread.h>
#include "config.h"

typedef struct _thread_argu {
	int connfd;
	int srv_id;
}THREAD_ARGU;


#define max_log_chunk_cnt 1024*512

#define bucket_num max_log_chunk_cnt/(1024*2)
#define entry_per_bucket max_log_chunk_cnt/bucket_num

int* newest_chunk_log_order=malloc(sizeof(int)*bucket_num*entry_per_bucket*2);//it records the mapping between the newest version of chunk_id and the logged order;
                                                  //chunk_id stored are sored in ascending order

int log_chunk_cnt; //it records the number of chunks logged


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
   newest_chunk_log_order[bucket_id*entry_per_bucket*2+2*i+1]=log_chunk_cnt;
   
}



// write the received data in a log file
// update the offset of the wrtten chunk 
void* log_write(void *ptr){

   THREAD_ARGU ta=*(THREAD_ARGU *)ptr;

   int connfd=ta.connfd;
   int svc_id=ta.srv_id;
   int ret;
   int log_order;
   int position;

   //append the data into a log file, record the offset 
   FILE* fd=open("log_file","a");

   //offset can be obtained from the chunk_log_file_record mapping
   ret=fwrite(fd, td->buff, chunk_size);
   if(ret!=chunk_size)
   	printf("write_log_file_error!\n");

   log_order=log_chunk_cnt; // record the log order
   log_chunk_cnt++;

   update_loged_chunks(td->chunk_id); //find the position in newest_chunk_log_order and update the log order
   
   fclose(fd);
   close(connfd);

}


void partial_encoding(){






}


/**
 * Start server service on specified port
 */
int main(int argc, char** argv){

    if(argc != 2){
	printf("Usage: ./server <port> \n");
	return 0;
    }
	signal(SIGPIPE,SIG_IGN);

	int srv_cnt=0;
	int server_socket;
	int opt=1;

	int read_size;
	int recv_len;

	pthread_t tid;
	int port = atoi(argv[1]);

	int  connfd = 0;
	
	THREAD_ARGU* ta;	

	//initial socket information

	struct sockaddr_in server_addr;
	bzero(&server_addr,sizeof(server_addr)); 
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = htons(INADDR_ANY);
	server_addr.sin_port = htons(port);

	server_socket = socket(PF_INET,SOCK_STREAM,0);
	if( server_socket < 0){
	   printf("Create Socket Failed!");
	   exit(1);
		  }

	printf("socket retrive success\n");

	setsockopt(server_socket,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

	if(bind(server_socket,(struct sockaddr*)&server_addr,sizeof(server_addr))){
		  printf("Server Bind Port : %d Failed!", port); 
		  exit(1);
		 }

	if(listen(server_socket,100) == -1){
		printf("Failed to listen.\n");
		return -1;
	}


	//init the sender info
	struct sockaddr_in sender_addr; 
	socklen_t length=sizeof(sender_addr);

    //init the hash_bucket
	log_chunk_cnt=0;
	memset(newest_chunk_log_order, -1, max_log_chunk_cnt*2);

    //init the recv info
    TRANSMIT_DATA* td = malloc(sizeof(TRANSMIT_DATA));
	char *recv_buff = malloc(sizeof(TRANSMIT_DATA));

	while(1){
		connfd = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
		printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));
		
		ta = (THREAD_ARGU*)malloc(sizeof(THREAD_ARGU));
		ta->connfd = connfd;
		srv_cnt++;
		ta->srv_id = srv_cnt;
		// we should differentiate the accepted data in the new data or the data delta for aggregation
		// judge it the data comes from client (log-write) or servers (partial encoding)

        //read data from socket
        recv_len=0; 
        while(recv_len < sizeof(TRANSMIT_DATA)){
	       read_size=read(connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA));
	       recv_len+=read_size;
   	    }

       //transform to the structure
       memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

       //validate the data 
       printf("recv: td->chunk_id=%d\n", td->chunk_id);
	   printf("recv: td->data_type=%d\n", td->data_type);

       //case#1:if it is new data, then log it
	   if(td->data_type==1)
		   pthread_create(&tid,NULL,log_write,ta);
	   
       // case #2: partial_encoding
       // pthread_create(&tid,NULL,partial_encoding,ta);

	}

	free(td);
	free(recv_buff);


  	return 0;

}

