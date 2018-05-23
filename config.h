#ifndef _CONFIG_H
#define _CONFIG_H

#define GTWY_OPEN             0

/* erasure coding settings */
#define data_chunks           6
#define num_chunks_in_stripe  9 //n in erasure coding
#define chunk_size            1024*1024 // suppose the chunk size is 16MB

/* configurations of data centers */
#define total_nodes_num       9 //we set the number of chunks as the number of nodes in the local-cluster evaluation
#define max_chunks_per_rack   3
#define rack_num              3
#define node_num_per_rack     total_nodes_num/rack_num //we currently assume that each rack is composed of a constant number of nodes

/* varied configuration in evaluation*/
#define max_updt_strps        100 //it defines the maximum stripes to be updated before invoking delta commit

/*log table*/
#define max_log_chunk_cnt     1000
#define bucket_num            50
#define entry_per_bucket      max_log_chunk_cnt/bucket_num
#define max_num_store_chunks  1024*1024*10 //it denotes the maximum number of data chunks allowed to stored on a node
#define SERVER_PORT           2222

/* the number of kept replicas in CAU*/
//#define cau_num_rplc        (num_chunks_in_stripe-data_chunks)/2
#define cau_num_rplc          1

#define stripe_num            300000
#define string_len            100
#define w                     8
#define strlen                100

/* cluster settings */
#define ip_len                20
#define upper                 9999999

/* define update methods*/
#define CAU                   1
#define PARIX                 2
#define BASELINE              3

/* the operation type used in transmission  */
#define DATA_UPDT             1
#define DATA_PE               2 //it happens when the internal node commits the pse data to the parity node 
#define DATA_COMMIT           3
#define DELTA                 4 //it happens when the leaf node commits its data delta to the internal node
#define CMMT_CMLT             5 //it means that the commit is completed
#define DATA_LOG              6
#define UPDT_CMLT             7
#define LOG_CMLT              8
#define DATA_MVMNT            9
#define CMD_MVMNT             10
#define PRTY_UPDT             11
#define PRTY_UPDT_CMPLT       12
#define PRTY_LOG              13
#define TERMINATE             14
#define UPDT_REQ              15
#define MVMT_CMLT             16

//the role of each node in parity-delta approach
#define DATA_LEAF             1
#define DATA_INTERNAL         2
#define PARITY                3
#define IN_CHNK               4
#define OUT_CHNK              5

/* the roles of parity in data-delta-approach*/
#define PRTY_INTERNAL         6
#define PRTY_LEAF             7

/* the operations in PARIX*/
#define PARIX_UPDT            1
#define PARIX_CMMT            2
#define PARIX_OLD             3
#define PARIX_LGWT            4
#define PARIX_CMLT_CMMT       5
#define PARIX_UPDT_CMLT       6
#define PARIX_NEED_OLD_DT     7
#define PARIX_LOG_OLD_DT      8

/* commit approach */
#define DATA_DELTA_APPR       1
#define PARITY_DELTA_APPR     2
#define DIRECT_APPR           3

/*the port number*/ 
#define MAX_PORT_NUM          65535
#define MIN_PORT_NUM          1111
#define NO_DEFINED_PORT       -1

/*port num*/
#define UPDT_PORT             22222
#define UPDT_ACK_PORT         6666
#define LOG_ACK_PORT          5555
#define PARIX_UPDT_PORT       3333
#define CMMT_PORT             5566
#define MVMT_PORT             6677

#define head_size             16
#define UPDT_DATA             1
#define ACK_INFO              2
#define CMD_INFO              3
#define REQ_INFO              4
#define METADATA_INFO         5


#define num_tlrt_strp 10 //it is to avoid the last write may cause the num_updt_stripes > max_updt_strps, so we have to consider this case

typedef struct _transmit_data{

    int send_size;
    int op_type; // DATA_UPDT for new data; DATA_COMMIT for partial encoding
    int stripe_id; //it will be used in commit
    int data_chunk_id;//new data or data_delta
    int updt_prty_id; //-1 in DATA_UPDT
    int num_recv_chks_itn; //it is used in the partial encoding to specify how many chunks to receive for a internal node
    int num_recv_chks_prt;
    int port_num;          //we specify port_num in commit
    int prty_delta_app_role; //the role is fixed for a given commit approach
    int data_delta_app_prty_role; //if a parity chunk is an internal node in a rack, then it will always be the internal node in the data-delta approach
    int chunk_store_index;
    int updt_prty_nd_id[num_chunks_in_stripe-data_chunks]; 
    int updt_prty_store_index[num_chunks_in_stripe-data_chunks];
    int commit_app[num_chunks_in_stripe-data_chunks];        //parity-delta-first or data-delta-first
    int parix_updt_data_id[data_chunks];                     //used to record the number of updated chunks in a stripe
    char next_ip[ip_len];
    char sent_ip[ip_len]; //the ip addr to send the data 
    char from_ip[ip_len]; //record the ip addr of the sender 
    char next_dest[num_chunks_in_stripe-data_chunks][ip_len]; //it records the next dest ip addr to send the td->buff in the m parity chunks' renewals
    //for example, the next_addr of the leaf node in parity delta approach should the internal node 
    char buff[chunk_size];
}TRANSMIT_DATA;

typedef struct _aggt_send_data{

    int this_data_id;
    int this_rack_id;
    int this_stripe_id;
    int updt_prty_id;
    int data_delta_num;
    char next_ip[ip_len];
    int recv_delta_id[data_chunks]; 
    int commit_app[num_chunks_in_stripe-data_chunks];        //parity-delta-first or data-delta-first
    char data_delta[chunk_size];

}AGGT_SEND_DATA;


typedef struct _ack_data{

    int send_size;
    int op_type;
    int stripe_id;
    int data_chunk_id;
    int updt_prty_id;
    int port_num;
    char next_ip[ip_len];

}ACK_DATA;


typedef struct _cmd_data{

    int send_size;
    int op_type; // DATA_UPDT for new data; DATA_COMMIT for partial encoding
    int stripe_id; //it will be used in commit
    int data_chunk_id;//new data or data_delta
    int updt_prty_id; //-1 in DATA_UPDT
    int num_recv_chks_itn; //it is used in the partial encoding to specify how many chunks to receive for a internal node
    int num_recv_chks_prt;
    int port_num;          //we specify port_num in commit
    int prty_delta_app_role; //the role is fixed for a given commit approach
    int data_delta_app_prty_role; //if a parity chunk is an internal node in a rack, then it will always be the internal node in the data-delta approach
    int chunk_store_index;
    int updt_prty_nd_id[num_chunks_in_stripe-data_chunks]; 
    int updt_prty_store_index[num_chunks_in_stripe-data_chunks];
    int commit_app[num_chunks_in_stripe-data_chunks];        //parity-delta-first or data-delta-first
    int parix_updt_data_id[data_chunks];                     //used to record the number of updated chunks in a stripe
    char next_ip[ip_len];
    char sent_ip[ip_len]; //the ip addr to send the data 
    char from_ip[ip_len]; //record the ip addr of the sender 
    char next_dest[num_chunks_in_stripe-data_chunks][ip_len]; //it records the next dest ip addr to send the td->buff in the m parity chunks' renewals
    //for example, the next_addr of the leaf node in parity delta approach should the internal node 
}CMD_DATA;


typedef struct _updt_req_data{

    int op_type;
    int local_chunk_id;

}UPDT_REQ_DATA;


typedef struct _meta_info{

    int stripe_id;
    int data_chunk_id;
    int port_num;
    int chunk_store_index;
    int updt_prty_nd_id[num_chunks_in_stripe-data_chunks]; 
    int updt_prty_store_index[num_chunks_in_stripe-data_chunks];
    int if_first_update;
    char next_ip[ip_len];

}META_INFO;



typedef struct _recv_process_data{//it is used in receive data by multiple threads

    int connfd; 
    int recv_id;

}RECV_PROCESS_DATA;

typedef struct _recv_process_prty{//it is used in receive data by multiple threads

    int connfd; 
    int recv_id;
    int prty_delta_role;
    int prty_nd_id;

}RECV_PROCESS_PRTY;


int  global_chunk_map[stripe_num*num_chunks_in_stripe];
int* encoding_matrix; //the encoding coefficient matrix
int  num_updt_strps;
int  newest_chunk_log_order[max_log_chunk_cnt*2];//it records the mapping between the newest version of chunk_id and the logged order
int  old_chunk_log_order[max_log_chunk_cnt*2]; //it records the old data that are logged for parity updates in parix
int  new_log_chunk_cnt; //it records the number of chunks logged
int  old_log_chunk_cnt;
int  chunk_store_order[total_nodes_num*max_num_store_chunks]; //it records the data chunks stored in ascending order on a node in data_file
int  num_store_chunks[total_nodes_num];//it records the number of stored chunks, including data and parity chunks, on a node

int  intnl_recv_data_id[data_chunks];
char intnl_recv_data[chunk_size*data_chunks];

char in_chunk[chunk_size];
char out_chunk[chunk_size];

/* the global arrays used in parallel receiving acks */
int   updt_cmlt_count[num_chunks_in_stripe-data_chunks];
int   need_old_dt_count[num_chunks_in_stripe-data_chunks];
int   prty_log_cmplt_count[num_chunks_in_stripe-data_chunks];
int   commit_count[num_chunks_in_stripe-data_chunks];
int   mvmt_count[data_chunks];

/* global variables in CAU client */
int mark_updt_stripes_tab[(max_updt_strps+num_tlrt_strp)*(data_chunks+1)]; //it records the updated data chunks and their stripes, the data_chunk column stores the data, while the data_chunk-th column store the stripe_id
int prty_log_map[stripe_num*data_chunks];

/* gateway switch */
int if_gateway_open;

#endif
