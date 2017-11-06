#ifndef _CONFIG_H
#define _CONFIG_H


#define stripe_num            1000000
#define test_times            20 // the iteration steps
#define upper                 9999999
#define max_updt_strps        100 //it defines the maximum stripes to be updated before invoking delta commit

/* erasure coding settings */
#define data_chunks           3
#define num_chunks_in_stripe  5 //n in erasure coding
#define chunk_size            1024*64 // suppose the chunk size is 16MB
#define string_len            100
#define w                     8
#define strlen                100

/* cluster settings */
#define total_nodes_num       6
#define max_chunks_per_rack   2
#define rack_num              3
#define ip_len                50

/* the operation type used in transmission  */
#define DATA_UPDT             1
#define DATA_PE               2 //it happens when the internal node commits the pse data to the parity node 
#define DATA_COMMIT           3
#define DELTA                 4 //it happens when the leaf node commits its data delta to the internal node
#define CMMT_CMLT             5 //it means that the commit is completed
#define DATA_LOG              6
#define LOG_CMLT              7
#define DATA_MVMNT            8
#define CMD_MVMNT             9

//the role of each node in data commit
#define LEAF                  1
#define INTERNAL              2
#define PARITY                3
#define IN_CHNK               4
#define OUT_CHNK              5

#define PARIX_UPDT            1
#define PARIX_CMMT            2
#define PARIX_OLD             3
#define PARIX_LGWT            4
#define PARIX_CMLT_CMMT       5
#define PARIX_UPDT_CMLT       6
#define PARIX_NEED_OLD_DT     7
#define PARIX_LOG_OLD_DT      8

/*the port number*/ 
#define MAX_PORT_NUM          65535
#define MIN_PORT_NUM          1111

/*port num*/
#define UPDT_PORT             2222
#define UPDT_ACK_PORT         2233
#define LOG_ACK_PORT          3322
#define PARIX_UPDT_PORT       3333
#define MVMT_PORT             3344

/*log table*/
#define max_log_chunk_cnt     200
#define bucket_num            20
#define entry_per_bucket      max_log_chunk_cnt/bucket_num
#define max_num_store_chunks  1024*1024 //it denotes the maximum number of data chunks allowed to stored on a node
#define SERVER_PORT           1111

typedef struct _transmit_data{

	int op_type; // DATA_UPDT for new data; DATA_COMMIT for partial encoding
	int stripe_id; //it will be used in commit
	int data_chunk_id;//new data or data_delta
	int updt_prty_id; //-1 in DATA_UPDT
	int num_recv_chks_itn; //it is used in the partial encoding to specify how many chunks to receive for a internal node
	int num_recv_chks_prt;
	int port_num;          //we specify port_num in commit
	int role;
	int chunk_store_index;
	int updt_prty_nd_id[num_chunks_in_stripe-data_chunks];
	char next_ip[50];
	char sent_ip[50]; //the ip addr to send the data 
	char buff[chunk_size];
	
}TRANSMIT_DATA;



typedef struct _thread_cmd_data{

	int op_type; // DATA_UPDT for new data; DATA_COMMIT for partial encoding
	int stripe_id; //it will be used in commit
	int data_chunk_id;//new data or data_delta
	int updt_prty_id; //-1 in DATA_UPDT
	int num_recv_chks_itn; //it is used in the partial encoding to specify how many chunks to receive for a internal node
	int num_recv_chks_prt; //it is used to tell the parity chunk how many pse chunks it should collect
	int port_num;
	int role; //the role (leaf, internal, parity) 
	int chunk_store_index;
	int updt_prty_nd_id[num_chunks_in_stripe-data_chunks];
	char next_ip[50]; //tell the node that receive the data where to send the data next, for example, leaf node will send the data next to the internal node
	char sent_ip[50]; //the ip addr to send the data 
	char buff[chunk_size];
	
}THREAD_CMD_DATA;


typedef struct _recv_process_data{//it is used in receive data by multiple threads

    int connfd; 
	int recv_id;

}RECV_PROCESS_DATA;

int  global_chunk_map[stripe_num*num_chunks_in_stripe];
char intnl_recv_data[chunk_size*data_chunks];
int* encoding_matrix; //the encoding coefficient matrix
int  num_updt_strps;
int  newest_chunk_log_order[max_log_chunk_cnt*2];//it records the mapping between the newest version of chunk_id and the logged order
int  old_chunk_log_order[max_log_chunk_cnt*2]; //it records the old data that are logged for parity updates in parix
int  new_log_chunk_cnt; //it records the number of chunks logged
int  old_log_chunk_cnt;
int  chunk_store_order[total_nodes_num*max_num_store_chunks]; //it records the data chunks stored in ascending order on a node in data_file
int  num_store_chunks[total_nodes_num];//it records the number of stored chunks, including data and parity chunks, on a node

char in_chunk[chunk_size];
char out_chunk[chunk_size];

#endif
