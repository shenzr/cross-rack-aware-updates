#ifndef _CONFIG_H
#define _CONFIG_H

#define total_nodes_num 10
#define data_chunks 4
#define num_chunks_in_stripe 7 //n in erasure coding
#define rack_num 3
#define stripe_num 100
#define test_times 20 // the iteration steps
#define upper 9999999

#define BUFFER_SIZE 1024*16 // the buffer size in socket communication is set as 16KB
#define chunk_size 1024*64 // suppose the chunk size is 16MB
#define string_len 100
#define w 8


typedef struct _transmit_data{
	int chunk_id;
	int data_type; //new data or data_delta
	char buff[chunk_size];
}TRANSMIT_DATA;


#endif
