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


// write the received data in a log file
// update the offset of the wrtten chunk 
void* log_write(void *ptr){

   THREAD_ARGU ta=*(THREAD_ARGU *)ptr;

   int connfd=ta.connfd;
   int svc_id=ta.srv_id;
   int read_size;

   TRANSMIT_DATA* td = malloc(sizeof(TRANSMIT_DATA));

   //read data from socket
   read_size=read(connfd, td, sizeof(TRANSMIT_DATA)!=sizeof(TRANSMIT_DATA));
   if(read_size==-1){

	  printf("read socket error!\n");
	  exit(0);

   	}

   //validate the data 
   printf("recv: td->chunk_id=%d\n", td->chunk_id);

   //write the data into a log file, record the offset


   close(connfd);
   free(td);

}


void partial_encoding(){






}


/**
 * Start server service on specified port
 * Server receive segment fingerprints from client
 * Deduplicate based on these fingerprints
 * Make decision on how to distribute the unique segments
 * Inform client of the decision on distribution
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


	while(1){
		connfd = accept(server_socket, (struct sockaddr*)&sender_addr,&length);
		printf("receive connection from %s\n",inet_ntoa(sender_addr.sin_addr));
		
		ta = (THREAD_ARGU*)malloc(sizeof(THREAD_ARGU));
		ta->connfd = connfd;
		srv_cnt++;
		ta->srv_id = srv_cnt;
		// we should differentiate the accepted data in the new data or the data delta for aggregation
		// judge it the data comes from client (log-write) or servers (partial encoding)

		// to be added ...........



		

		// case#1: log_write
		pthread_create(&tid,NULL,log_write,ta);

        // case #2: partial_encoding
        // pthread_create(&tid,NULL,partial_encoding,ta);

	}

    	return 0;
 

}

