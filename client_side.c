#define _GNU_SOURCE 


#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include "config.h"


int main(int argc, char** argv){

   int ret;
   int on;
   int SERVER_PORT=7777;
   int sent_len;

   char* server_ip="192.168.0.18"; //node8 is the server

   TRANSMIT_DATA* td=malloc(sizeof(TRANSMIT_DATA));

   // initiate td for example
   td->chunk_id=66;
   td->data_type=1;
   memset(td->buff,'1',chunk_size);

   //set client_addr info
   struct sockaddr_in client_addr;
   bzero(&client_addr,sizeof(client_addr)); 
   client_addr.sin_family = AF_INET;	
   client_addr.sin_addr.s_addr = htons(INADDR_ANY);
   client_addr.sin_port = htons(0);    
   
   //create client socket
   int client_socket = socket(AF_INET,SOCK_STREAM,0);
   on=1;
   ret = setsockopt( client_socket, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on) );
   
   if( client_socket < 0)
   {
	   printf("Create Socket Failed!\n");
	   exit(1);
   }
   
   //combine client_socket with client_addr
   if( bind(client_socket,(struct sockaddr*)&client_addr,sizeof(client_addr)))
   {
	   printf("Client Bind Port Failed!\n"); 
	   exit(1);
   }
   
   //set server_addr info
   struct sockaddr_in server_addr;
   bzero(&server_addr,sizeof(server_addr));
   server_addr.sin_family = AF_INET;
   
   //printf("---2\n");
   
   if(inet_aton(server_ip,&server_addr.sin_addr) == 0) 
   {
	   printf("Server IP Address Error!\n");
	   exit(1);
   }
   server_addr.sin_port = htons(SERVER_PORT);
   socklen_t server_addr_length = sizeof(server_addr);
   
   //printf("---3\n");
   
   //connect server
   while(connect(client_socket,(struct sockaddr*)&server_addr, server_addr_length) < 0);

   printf("connect success!\n");

   //init sent_buff
   char *sent_buff=malloc(sizeof(TRANSMIT_DATA));
   memcpy(sent_buff,td,sizeof(TRANSMIT_DATA));

   sent_len=0;

   while(sent_len < sizeof(TRANSMIT_DATA)){
      ret=write(client_socket, sent_buff+sent_len, sizeof(TRANSMIT_DATA));
	  sent_len+=ret;
   	}
   
   free(td);
   free(sent_buff);

}

