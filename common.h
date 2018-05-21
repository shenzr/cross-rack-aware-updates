#define _GNU_SOURCE 

#include "config.h"

extern void print_array(int row, int col, int *array);
extern void read_chunk_map(char* map_file);
extern int find_max_array(int* array, int n);
extern int get_rack_id(int node_id);
extern int init_client_socket(int port_num);
extern void send_data(TRANSMIT_DATA *td, char *server_ip, int port_num, ACK_DATA* ack,  CMD_DATA* cmd, int send_data_type);
extern int init_server_socket(int port_num);
extern int update_loged_chunks(int given_chunk_id);
extern void trnsfm_char_to_int(char *char_data, long long *data);
extern void new_strtok(char string[], char divider, char result[]);
extern void gene_radm_buff(char* buff, int len);
extern int count_non_ngtv(int* arr, int len);
extern void send_updt_data(TRANSMIT_DATA* td, char* server_ip, int port_num);
extern int get_local_node_id();
extern int get_local_chunk_id(int stripe_id);
extern void bitwiseXor(char* result, char* srcA, char* srcB, int length);
extern void listen_ack(ACK_DATA* cmt_ntf_td, char* recv_buff, int stripe_id, int updt_dt_id, int updt_prty_id, int port_num, int op_type);
extern int obtain_encoding_coeff(int given_chunk_id, int prtyid_to_update);
extern void encode_data(char* data, char* pse_coded_data, int chunk_id, int updt_prty);
extern void flush_new_data(int stripe_id, char* new_data, int global_chunk_id, int stored_index); 
extern void aggregate_data(char* data_delta, int num_recv_chnks, char* ped);
extern int get_local_chunk_id(int stripe_id);
extern void get_chunk_store_order();
extern void send_ack(int stripe_id, int dt_id, int prty_id, char* destined_ip, int port_num, int op_type);
extern int locate_store_index(int node_id, int global_chunk_id);
extern void para_recv_data(int stripe_id, int num_recv_chnks, int port_num, int flag_tag);
extern void write_new_data(char* write_buff, int store_index);
extern void read_old_data(char* read_buff, int store_index);
extern int sum_array(int num, int* arr);
extern void* send_updt_data_process(void* ptr);
extern void para_send_dt_prty(TRANSMIT_DATA* td, int op_type, int num_updt_prty, int port_num);
extern void para_recv_ack(int stripe_id, int num_recv_chnks, int port_num);
extern void log_write(char* filename, TRANSMIT_DATA* td);
extern void gateway_forward(TRANSMIT_DATA* td, char* sender_ip);
extern int get_node_id(char* given_ip);
extern void GetLocalIp(char* local_ip);
extern void read_log_data(int local_chunk_id, char* log_data, char* filename);
extern void evict_log_dt(int* log_table, int logcal_data_id);
extern int* reed_sol_vandermonde_coding_matrix(int data, int prty, int word_len);
extern double ceil(double x);
extern int find_max_array_index(int* array, int n);
extern unsigned int RSHash(char* str, unsigned int len);
extern void print_amazon_vm_info(char* node_ip);
extern void gateway_forward_updt_data(TRANSMIT_DATA* td, char* sender_ip);
extern void gateway_forward_ack_info(ACK_DATA* ack);
extern void gateway_forward_cmd_data(CMD_DATA* cmd);
extern void send_req(UPDT_REQ_DATA* req, char* server_ip, int port_num, META_INFO* metadata, int info_type);
extern void connect_metaserv(int chunk_id, META_INFO* metadata);

extern int   nodes_in_racks[rack_num];
extern char* node_ip_set[total_nodes_num];
extern char* mt_svr_ip;
extern char* gateway_ip;
extern char* region_name[rack_num];
extern char* client_ip;
