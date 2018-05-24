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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>
#include <sys/time.h>

#include "common.h"
#include "config.h"

//chunk_id stored are sored in ascending orde
int if_commit_start;
int data_delta_role;
char pse_prty[data_chunks*chunk_size];

/*
 * Given the stripe_id and the parity chunk id, this function returns the node id where the parity chunk to be updated resides on
 */ 
int locate_prty_node_id(int stripe_id, int prty_chunk_id){

    return global_chunk_map[stripe_id*num_chunks_in_stripe+prty_chunk_id];

}


/*
 * This function writes a chunk to an append-only file 
 */
void cau_log_write(TRANSMIT_DATA* td){

    // move fd at the bottom of the file
    int local_chunk_id;
    int ret;
    local_chunk_id=td->stripe_id*data_chunks+td->data_chunk_id;

    // append the data and update the log records
    log_write("cau_log_file", td);
    ret=update_loged_chunks(local_chunk_id); 
    new_log_chunk_cnt++;

}

/* 
 * This function reads an old data and a new data, and calculates the data delta
 */ 
void cau_read_cal_data_delta(int stripe_id, char* data_delta, int local_chunk_id, int store_index){

    char *log_data=malloc(sizeof(char)*chunk_size);
    char* ori_data=malloc(sizeof(char)*chunk_size);

    // calculate the data delta chunk
    read_log_data(local_chunk_id, log_data, "cau_log_file");

    // calculate data delta
    bitwiseXor(data_delta, ori_data, log_data, chunk_size);

    // in-place write the new data chunk
    write_new_data(log_data, local_chunk_id);

    free(log_data);
    free(ori_data);
}

/*
 * This function is performed at the data node, describing how a data node handles data update requests 
 */ 
void cau_server_updte(TRANSMIT_DATA* td){

    int ret;
    int sum_cmplt;
    int prty_node_id;

    // if it is the first update after commit, then delete the logged data first
    if(if_commit_start==1){

        // when finishing the commit, return to the default settings
        ret=truncate("cau_log_file", 0);
        if(ret!=0){
            printf("truncate fails\n");
            exit(1);
        }

        // re-check the file size 
        struct stat stat_info;
        stat("cau_log_file", &stat_info);

        new_log_chunk_cnt=0;
        if_commit_start=0;

    }

    // write the data in an append-only file 
    cau_log_write(td);

    // if the number of replica to stored is one, then send the replica to one parity node in another rack
    if(cau_num_rplc==1){

        td->op_type=DATA_LOG;
        prty_node_id=td->updt_prty_nd_id[0];
        memcpy(td->sent_ip, node_ip_set[prty_node_id], ip_len);
		memcpy(td->next_ip, td->sent_ip, ip_len);

		if(GTWY_OPEN)
			send_data(td, gateway_ip, td->port_num, NULL, NULL, UPDT_DATA);

		else 
			send_data(td, td->sent_ip, td->port_num, NULL, NULL, UPDT_DATA);

        ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
        char* recv_buff=(char*)malloc(sizeof(ACK_DATA));

        // listen ack 
        listen_ack(ack, recv_buff, td->stripe_id, td->data_chunk_id, td->updt_prty_id, LOG_ACK_PORT, LOG_CMLT);

        free(ack);
        free(recv_buff);
    }

    else if(cau_num_rplc > 1){

        // send the new data to cau_num_rplc parity nodes in parallel
        // listen the ack in parallel 
        memset(prty_log_cmplt_count, 0, sizeof(int)*(num_chunks_in_stripe-data_chunks));
        para_send_dt_prty(td, DATA_LOG, cau_num_rplc, td->port_num, LOG_ACK_PORT);

        // check the ack number 
        sum_cmplt=sum_array(num_chunks_in_stripe-data_chunks, prty_log_cmplt_count);
        if(sum_cmplt!=cau_num_rplc){
            printf("update error! sum_cmplt=%d\n", sum_cmplt);
            exit(1);
        }
    }

    // send ack to client if the update finishes
    send_ack(td->stripe_id, td->data_chunk_id, -1, client_ip, UPDT_ACK_PORT, LOG_CMLT);

}

/*
 * This function will send delta to a node in the same rack. 
 * We define a parity node as a parity leaf if this parity node has to send delta to 
 * a node (called internal node) within the same rack for aggregation in parity-delta commit.
 * So if it is leaf node, read the data and send the data delta to the internal node
 */
void cau_prty_delta_app_leaf_action(TRANSMIT_DATA* td, int updt_prty_rack_id, int rack_id, int updt_prty_id, char* data_delta){

    int temp_node_id;
    int temp_rack_id;

    // initiallize the structure
    TRANSMIT_DATA* delta = (TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));   
    delta->send_size=sizeof(TRANSMIT_DATA);
    delta->op_type=DELTA;
    delta->stripe_id=td->stripe_id;
    delta->data_chunk_id=td->data_chunk_id;
    delta->updt_prty_id=td->updt_prty_id;
    delta->prty_delta_app_role=DATA_LEAF;
    delta->port_num=td->port_num;

    memcpy(delta->next_ip, td->next_dest[updt_prty_id], ip_len);
    memcpy(delta->buff, data_delta, chunk_size);

    temp_node_id=get_node_id(td->next_ip);
    temp_rack_id=get_rack_id(temp_node_id);

    if((GTWY_OPEN==1) && (temp_rack_id!=rack_id))
        send_data(delta, gateway_ip, SERVER_PORT, NULL, NULL, UPDT_DATA);

    else 
        send_data(delta, td->next_dest[updt_prty_id], SERVER_PORT+data_chunks+updt_prty_id, NULL, NULL, UPDT_DATA);

    free(delta);

}

/*
 * This function is executed by the internal nodes. 
 * It aggregates the parity delta chunks and sends the partial-encoded result to the parity nodes.
 */
void* internal_aggr_send_process(void* ptr){

    AGGT_SEND_DATA asd=*(AGGT_SEND_DATA*)ptr; 

    int i;
    int temp_node_id, temp_rack_id;

    char* intl_prty_delta=(char*)malloc(sizeof(char)*chunk_size);
    char* recv_prty_delta=(char*)malloc(sizeof(char)*chunk_size*asd.data_delta_num);

    // encode the data delta of the internal node
    encode_data(asd.data_delta, intl_prty_delta, asd.this_data_id, asd.updt_prty_id);

    // encode the data deltas of the leave nodes 
    for(i=0; i<asd.data_delta_num; i++)
        encode_data(intnl_recv_data+i*chunk_size, recv_prty_delta, asd.recv_delta_id[i], asd.updt_prty_id);

    // aggregate the parity delta chunks from leaves 
    aggregate_data(intl_prty_delta, asd.data_delta_num, recv_prty_delta);

    // send the parity delta chunks
    TRANSMIT_DATA* ped = (TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
    ped->send_size=sizeof(TRANSMIT_DATA);
    ped->op_type=DATA_PE;
    ped->stripe_id=asd.this_stripe_id;
    ped->data_chunk_id=asd.this_data_id;
    ped->updt_prty_id=asd.updt_prty_id;
    ped->port_num=SERVER_PORT+data_chunks+ped->updt_prty_id;

    for(i=0; i<num_chunks_in_stripe-data_chunks; i++)
        ped->commit_app[i]=asd.commit_app[i];

    memcpy(ped->buff, intl_prty_delta, chunk_size);
    memcpy(ped->next_ip, asd.next_ip, ip_len);

    temp_node_id=get_node_id(asd.next_ip);
    temp_rack_id=get_rack_id(temp_node_id);

    // if the gateway server is defined, then send the parity delta to the gateway first once the parity nodes are in other racks
    // otherwise, directly send the parity delta to the parity nodes
    if((GTWY_OPEN==1) && (temp_rack_id!=asd.this_rack_id))
        send_data(ped, gateway_ip, SERVER_PORT, NULL, NULL, UPDT_DATA);

    else 
        send_data(ped, ped->next_ip, SERVER_PORT+data_chunks+asd.updt_prty_id, NULL, NULL, UPDT_DATA);

    free(recv_prty_delta);
    free(ped);
    free(intl_prty_delta);

    return NULL;

}

/*
 * This function describes the funtionality of internal node in parity-delta commit. 
 * It receives deltas from the leaves within the same rack, aggregates the deltas, and sends the result to m parity nodes. 
 */ 
void cau_prty_delta_app_intnl_action(TRANSMIT_DATA* td, int updt_prty_rack_id, int rack_id, int updt_prty_id, char* data_delta){

    int stripe_id;
    int chunk_id;
    int prty_id;
    int count;
    int id;
    int recv_count;
    int i;
    int cddt_rack_id;
    int node_id;

    stripe_id=td->stripe_id;
    chunk_id=td->data_chunk_id;

    memset(intnl_recv_data_id, 0, sizeof(int)*data_chunks);

    // receive deltas in parallel 
    if(td->num_recv_chks_itn>=1)
        para_recv_data(stripe_id, td->num_recv_chks_itn, SERVER_PORT+data_chunks+updt_prty_id, 1);

    // for each parity chunk, if a parity chunk is recorded to be generated based on 
    // parity-delta commit approach, then generate the parity delta and send it to the corresponding parity node 
    AGGT_SEND_DATA* asd=(AGGT_SEND_DATA*)malloc(sizeof(AGGT_SEND_DATA)*(num_chunks_in_stripe-data_chunks));

    pthread_t aggregate_send_mt[num_chunks_in_stripe-data_chunks];

    count=0;
    for(prty_id=0; prty_id < num_chunks_in_stripe-data_chunks; prty_id++){

        node_id=td->updt_prty_nd_id[prty_id];
        cddt_rack_id=get_rack_id(node_id);

        // if the candidate rack is not the parity rack we focus on
        if(cddt_rack_id!=updt_prty_rack_id)
            continue;

        if(td->commit_app[prty_id]==PARITY_DELTA_APPR){

            asd[prty_id].this_data_id=chunk_id;
            memcpy(asd[prty_id].data_delta, data_delta, chunk_size);

            for(i=0; i<num_chunks_in_stripe-data_chunks; i++)
                asd[prty_id].commit_app[i]=td->commit_app[i];

            asd[prty_id].this_stripe_id=stripe_id;
            asd[prty_id].updt_prty_id=prty_id;
            asd[prty_id].this_rack_id=rack_id;
            asd[prty_id].data_delta_num=td->num_recv_chks_itn;

            memcpy(asd[prty_id].next_ip, node_ip_set[td->updt_prty_nd_id[prty_id]], ip_len);

            recv_count=0;
            for(id=0; id<data_chunks; id++){

                if(intnl_recv_data_id[id]==1){

                    asd[prty_id].recv_delta_id[recv_count]=id;
                    recv_count++;

                }
            }

            if(recv_count!=td->num_recv_chks_itn){

                printf("ERR: internal_recv_num!\n");
                exit(1);

            }

            pthread_create(&aggregate_send_mt[count], NULL, internal_aggr_send_process, (void *)(asd+prty_id));

            count++;

        }
    }

    for(i=0; i<count; i++)
        pthread_join(aggregate_send_mt[i], NULL);

    free(asd);

}

/*
 * This function encodes the data delta chunk and send the parity delta chunk to the parity nodes within the same rack
 */ 
void cau_direct_updt_action(TRANSMIT_DATA* td, int data_chunk_id, int rack_id, int prty_chunk_id, char* data_delta){

    char* pse_data=(char*)malloc(sizeof(char)*chunk_size);
    encode_data(data_delta, pse_data, data_chunk_id, prty_chunk_id);

    memcpy(td->buff, pse_data, chunk_size);
    send_data(td, td->next_dest[prty_chunk_id], SERVER_PORT+data_chunks+prty_chunk_id, NULL, NULL, UPDT_DATA);

    free(pse_data);
}

/*
 * This function is performed at the parity node to parallelly receive delta chunks and parity delta chunks.
 */ 
void* prty_recv_data_process(void* ptr){

    RECV_PROCESS_PRTY rpp=*(RECV_PROCESS_PRTY *)ptr;

    int recv_len;
    int read_size;
    int prty_rack_id;
    int tmp_prty_rack_id;
    int tmp_prty_node_id;
    int i;
    int count;

    char* recv_buff=(char*)malloc(sizeof(TRANSMIT_DATA));
    char* pse_coded_data=(char*)malloc(sizeof(char)*chunk_size);

    //receive data
    recv_len=0;
    while(recv_len < sizeof(TRANSMIT_DATA)){

        read_size=read(rpp.connfd, recv_buff+recv_len, sizeof(TRANSMIT_DATA)-recv_len);
        recv_len+=read_size;

    }

    // copy the data
    TRANSMIT_DATA* td=(TRANSMIT_DATA *)malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

    prty_rack_id=get_rack_id(rpp.prty_nd_id);

    // judge the commit_approach
    if(td->commit_app[td->updt_prty_id]==DATA_DELTA_APPR){

        // if the parity node is the internal node (i.e., the parity node to receive deltas of data nodes from other racks) 
        // then forward the deltas to other parity nodes within the rack 
        if(data_delta_role==PRTY_INTERNAL){

            pthread_t send_mt[num_chunks_in_stripe-data_chunks];

            TRANSMIT_DATA* send_td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA)*(num_chunks_in_stripe-data_chunks));

            //find the parity chunks in that rack
            count=0;
            for(i=0; i<num_chunks_in_stripe-data_chunks; i++){

                tmp_prty_node_id=td->updt_prty_nd_id[i];
                tmp_prty_rack_id=get_rack_id(tmp_prty_node_id);

                // if the parity node is in other racks, then continue
                if(prty_rack_id!=tmp_prty_rack_id)
                    continue;

                // if the parity node is this node, then continue
                if(tmp_prty_node_id==rpp.prty_nd_id)
                    continue;

                // initialize send_td[count] and send the data delta to the parity nodes within the same rack in parallel
                memcpy(send_td+count, td, sizeof(TRANSMIT_DATA));
                memcpy(send_td[count].sent_ip, node_ip_set[tmp_prty_node_id], ip_len);
                send_td[count].updt_prty_id=i;
                send_td[count].port_num=SERVER_PORT+data_chunks+i;

                pthread_create(&send_mt[count], NULL, send_updt_data_process, (void *)(send_td+count));

                count++;

            }

            for(i=0; i<count; i++)
                pthread_join(send_mt[i], NULL);

            free(send_td);

        }

        // calculate the parity delta
        encode_data(td->buff, pse_coded_data, td->data_chunk_id, td->updt_prty_id); 

        memcpy(pse_prty+rpp.recv_id*chunk_size, pse_coded_data, chunk_size);

    }

    // if the commit approach is parity-delta commit or direct commit, then we directly collect the recieved parity deltas for further aggregation
    else if (td->commit_app[td->updt_prty_id] == PARITY_DELTA_APPR || td->commit_app[td->updt_prty_id]==DIRECT_APPR)
        memcpy(pse_prty+rpp.recv_id*chunk_size, td->buff, chunk_size);


    free(td);
    free(recv_buff);
    free(pse_coded_data);

    return NULL;

}

/* 
 * This function describes the functionality performed by the parity node in delta commit. 
 */ 
void cau_prty_action(TRANSMIT_DATA* td, int rack_id, int server_socket){

    int global_chunk_id;
    int updt_prty_id;
    int stripe_id;
    int index;
    int i;
    char local_ip[ip_len];
    int prty_node_id;

    char* new_prty=(char*)malloc(sizeof(char)*chunk_size);

    stripe_id=td->stripe_id;
    updt_prty_id=td->updt_prty_id;
    global_chunk_id=stripe_id*num_chunks_in_stripe+data_chunks+updt_prty_id;
    data_delta_role=td->data_delta_app_prty_role;

    // get the node info and rack info
    memcpy(local_ip, td->sent_ip, ip_len);

    // find the parity node id
    for(i=0; i<total_nodes_num; i++){
        if(strcmp(node_ip_set[i], local_ip)==0)
            break;
    }

    prty_node_id=i;

    // read old parity
    read_old_data(new_prty, td->chunk_store_index);

    int* conn_fd=(int*)malloc(sizeof(int)*td->num_recv_chks_prt);
    pthread_t* pthread_mt=(pthread_t*)malloc(sizeof(pthread_t)*td->num_recv_chks_prt);
    memset(pthread_mt, 0, sizeof(pthread_t)*td->num_recv_chks_prt);

    // init the sender info
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    if(listen(server_socket,td->num_recv_chks_prt) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

    RECV_PROCESS_PRTY* rpp=(RECV_PROCESS_PRTY *)malloc(sizeof(RECV_PROCESS_PRTY)*td->num_recv_chks_prt);
    memset(rpp, 0, sizeof(RECV_PROCESS_PRTY)*td->num_recv_chks_prt);

    index=0;
    while(1){

        conn_fd[index] = accept(server_socket, (struct sockaddr*)&sender_addr, &length);

        rpp[index].connfd=conn_fd[index];
        rpp[index].recv_id=index;
        rpp[index].prty_delta_role=td->prty_delta_app_role;
        rpp[index].prty_nd_id=prty_node_id;

        // receive deltas in parallell 
        pthread_create(&pthread_mt[index], NULL, prty_recv_data_process, (void *)(rpp+index));

        // if receive enough deltas, then break 
        index++;
        if(index>=td->num_recv_chks_prt)
            break;

    }

    for(i=0; i<index; i++){
        pthread_join(pthread_mt[i], NULL);
        close(conn_fd[i]);
    }

    // calculate the new parity. We first copy the first chunk to the new_prty, 
    // and then aggregate the remaining index-1 partial encoded chunks in pse_prty
    memcpy(new_prty, pse_prty, chunk_size);
    aggregate_data(new_prty, index-1, pse_prty+chunk_size);

    // write the new parity 
    flush_new_data(stripe_id, new_prty, global_chunk_id, td->chunk_store_index); 

    // send ack to the metadata server
    send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, mt_svr_ip, CMMT_PORT, CMMT_CMLT);
    printf("Stripe-%d: Parity Node: Commit Completes in A Stripe!\n", td->stripe_id);

    free(new_prty);
    free(pthread_mt);
    free(rpp);
    free(conn_fd);

}

/*
 * This function is executed by the data and parity nodes, which performs the delta commit. 
 * 
 */ 
void cau_server_commit(CMD_DATA* cmd){

    TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));

    memcpy(td, cmd, sizeof(CMD_DATA)); 
    td->send_size=sizeof(TRANSMIT_DATA);

    printf("Stripe-%d Commit Starts:\n", td->stripe_id);

    int   local_chunk_id;
    int   its_stripe_id;
    int   node_id;
    int   rack_id;
    int   prty_cmmt; 
    int   prty_rack_id;
    int   server_socket;

    server_socket=init_server_socket(SERVER_PORT+td->updt_prty_id+data_chunks);

    node_id=get_local_node_id();
    rack_id=get_rack_id(node_id);
    its_stripe_id=td->stripe_id;
    local_chunk_id=its_stripe_id*data_chunks+td->data_chunk_id;

    int* mark_data_delta_cmmt=(int*)malloc(sizeof(int)*rack_num);
    int* mark_prty_delta_cmmt=(int*)malloc(sizeof(int)*rack_num);
    char* data_delta=(char*)malloc(sizeof(char)*chunk_size);

    memset(mark_data_delta_cmmt, 0, sizeof(int)*rack_num);
    memset(mark_prty_delta_cmmt, 0, sizeof(int)*rack_num);

    // if it is a parity node, then perform the parity action
    if(td->prty_delta_app_role==PARITY){

        //learn the number of deltas to be received 
        cau_prty_action(td, rack_id, server_socket);
        close(server_socket);

        free(data_delta);
        free(mark_data_delta_cmmt);
        free(mark_prty_delta_cmmt);
        free(td);

        return;
    }

    printf("DATA NODE:\n");

    // calculate data delta 
    cau_read_cal_data_delta(td->stripe_id, data_delta, local_chunk_id, td->chunk_store_index);

    // if it is a data chunk, we consider the commit of m parity chunks 
    for(prty_cmmt=0; prty_cmmt<num_chunks_in_stripe-data_chunks; prty_cmmt++){

        td->port_num=SERVER_PORT+data_chunks+prty_cmmt;
        td->updt_prty_id=prty_cmmt;
        prty_rack_id=get_rack_id(td->updt_prty_nd_id[prty_cmmt]);

        // identify the commit approach
        // if it uses data-delta commit approach, then send the delta to one parity node of other racks
        if(td->commit_app[prty_cmmt]==DATA_DELTA_APPR){

            printf("#DATA_DELTA COMMIT APPR:\n");

            // if the parity chunks in that rack have been committed
            if(mark_data_delta_cmmt[prty_rack_id]==1)
                continue;

            memcpy(td->buff, data_delta, chunk_size);

            // if the gateway server is established and the parity rack is a different rack, then send the delta to the gateway first
            // otherwise, send the delta to a parity node of that rack
            if((GTWY_OPEN==1) && (prty_rack_id!=rack_id)){

                memcpy(td->next_ip, td->next_dest[prty_cmmt], ip_len);
                send_data(td, gateway_ip, SERVER_PORT, NULL, NULL, UPDT_DATA);

            }

            else 
                send_data(td, td->next_dest[prty_cmmt], SERVER_PORT+data_chunks+prty_cmmt, NULL, NULL, UPDT_DATA);

            // mark that rack
            mark_data_delta_cmmt[prty_rack_id]=1;

        }

        //if it uses parity-delta commit approach, then send the delta to an a data node (called internal node) within the same rack
        else if(td->commit_app[prty_cmmt]==PARITY_DELTA_APPR){

            printf("#PARITY_DELTA COMMIT APPR:\n");

            // the internal node will send the parity delta to the parity chunks within the same rack parallelly. 
            // if this parity chunk has been updated by internal node
            if(mark_prty_delta_cmmt[prty_rack_id]==1)
                continue;

            memcpy(td->next_ip, td->next_dest[prty_cmmt], ip_len);

            // check the role of the data chunk 
            if(td->prty_delta_app_role==DATA_LEAF)
                cau_prty_delta_app_leaf_action(td, prty_rack_id, rack_id, prty_cmmt, data_delta);

            else if(td->prty_delta_app_role==DATA_INTERNAL)
                cau_prty_delta_app_intnl_action(td, prty_rack_id, rack_id, prty_cmmt, data_delta);

            mark_prty_delta_cmmt[prty_rack_id]=1;

        }

        // if the parity node is in the same rack, then directly send the delta to the parity node
        else if(td->commit_app[prty_cmmt]==DIRECT_APPR){

            printf("#DIRECT_APPR:\n");

            memcpy(td->next_ip, td->next_dest[prty_cmmt], ip_len);
            cau_direct_updt_action(td, td->data_chunk_id, rack_id, prty_cmmt, data_delta);

        }

    }

    //mark that the commit starts
    if(if_commit_start==0)
        if_commit_start=1;

    free(mark_data_delta_cmmt);
    free(data_delta);
    free(mark_prty_delta_cmmt);
    free(td);
    close(server_socket);


}

void cau_send_mvm_data(CMD_DATA* cmd){

    // read the cold data 
    char* cold_buff=(char*)malloc(sizeof(char)*chunk_size);

    read_old_data(cold_buff, cmd->chunk_store_index);

    TRANSMIT_DATA* td=(TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
    memcpy(td, cmd, sizeof(CMD_DATA));
    memcpy(td->buff, cold_buff, sizeof(char)*chunk_size);
    td->send_size=sizeof(TRANSMIT_DATA);

    // if the gateway server is established, then send the cold data to the gateway
    // else, send the data to the metadata server
    memcpy(td->next_ip, mt_svr_ip, ip_len);
	td->port_num=MVMT_PORT;

    if(GTWY_OPEN)
        send_data(td, gateway_ip, UPDT_PORT, NULL, NULL, UPDT_DATA);

    else 
        send_data(td, td->next_ip, td->port_num, NULL, NULL, UPDT_DATA);

    free(cold_buff);

}


void cau_write_hot_data(TRANSMIT_DATA* td){

    flush_new_data(td->stripe_id, td->buff, -1, td->chunk_store_index);
    send_ack(td->stripe_id, td->data_chunk_id, -1, mt_svr_ip, MVMT_PORT, MVMT_CMLT);

}


int main(int argc, char** argv){

    int server_socket;
    int read_size;
    int recv_len;
    int connfd = 0;
    int ret;
    int gateway_count;
    int send_size;
    int recv_data_type;

    char local_ip[ip_len];
    char sender_ip[ip_len];

    if_commit_start=0;

    // truncate the log file in case of it still has the log info in last evaluation
    FILE* fd;
    if((fd=fopen("cau_log_file", "r"))!=NULL){

        ret=truncate("cau_log_file", 0);
        if(ret!=0){
            printf("truncate fails\n");
            exit(1);
        }
    }

    // get local ip address
    GetLocalIp(local_ip);
    printf("local_ip=%s\n", local_ip);

    // initialize encoding coefficinets
    encoding_matrix=reed_sol_vandermonde_coding_matrix(data_chunks, num_chunks_in_stripe-data_chunks, w);

    // initialize socket information
    server_socket=init_server_socket(UPDT_PORT);
    if(listen(server_socket,100) == -1){
        printf("Failed to listen.\n");
        exit(1);
    }

    // initialize the sender info
    struct sockaddr_in sender_addr;
    socklen_t length=sizeof(sender_addr);

    // initialize the hash_bucket
    new_log_chunk_cnt=0;
    memset(newest_chunk_log_order, -1, sizeof(int)*max_log_chunk_cnt*2);

    num_updt_strps=0;

    // initialize the recv info
    TRANSMIT_DATA* td = (TRANSMIT_DATA*)malloc(sizeof(TRANSMIT_DATA));
    ACK_DATA* ack=(ACK_DATA*)malloc(sizeof(ACK_DATA));
    CMD_DATA* cmd=(CMD_DATA*)malloc(sizeof(CMD_DATA));

    char* recv_buff = (char*)malloc(sizeof(TRANSMIT_DATA));
    char* recv_head = (char*)malloc(head_size);

    gateway_count=0;

    while(1){

        connfd = accept(server_socket, (struct sockaddr*)&sender_addr, &length);

        memcpy(sender_ip, inet_ntoa(sender_addr.sin_addr), ip_len);
        memset(recv_head, '0', head_size);

        recv_len=0;
        //first read a part of data to determine the size of transmitted data
        read_size=read(connfd, recv_head, head_size);
        memcpy(&send_size, recv_head, sizeof(int));
        memcpy(recv_buff, recv_head, read_size);

        recv_len+=read_size;    

        while(recv_len < send_size){

            read_size=read(connfd, recv_buff+recv_len, send_size-recv_len);
            recv_len+=read_size;
        }

        recv_data_type=-1;

        // if it contains the updated data
        if(send_size==sizeof(TRANSMIT_DATA)){

            recv_data_type=UPDT_DATA;
            memcpy(td, recv_buff, sizeof(TRANSMIT_DATA));

        }

        // if it is an ack info
        else if(send_size==sizeof(ACK_DATA)){

            recv_data_type=ACK_INFO;
            memcpy(ack, recv_buff, sizeof(ACK_DATA));

        }

        // if it is a commit command
        else if(send_size==sizeof(CMD_DATA)){

            recv_data_type=CMD_INFO;
            memcpy(cmd, recv_buff, sizeof(CMD_DATA));
        }

        else{

            printf("ERR: unrecognized_send_size!\n");
            exit(1);

        }

        // if it is the gateway, then just forward the data to the destination node 
        if(((ret=strcmp(gateway_local_ip, local_ip))==0) && (GTWY_OPEN==1)){

            if(recv_data_type==UPDT_DATA)
                gateway_forward_updt_data(td, sender_ip);

            else if(recv_data_type==ACK_INFO)
                gateway_forward_ack_info(ack);

            else if(recv_data_type==CMD_INFO)
                gateway_forward_cmd_data(cmd);

            gateway_count++;
            if(gateway_count%1000==0)
                printf("have forwarded %d requests (including update, ack, and cmd)\n", gateway_count);

            close(connfd);
            continue;

        }

        if(td->op_type==DATA_UPDT && recv_data_type==UPDT_DATA)
            cau_server_updte(td);

        else if(td->op_type==DATA_LOG && recv_data_type==UPDT_DATA){//this is performed at the parity chunk side 

            cau_log_write(td);

            //check if it receives data from gateway
            if((strcmp(sender_ip, gateway_ip)==0) && (GTWY_OPEN==1))
                memcpy(sender_ip, td->from_ip, ip_len);

            send_ack(td->stripe_id, td->data_chunk_id, td->updt_prty_id, sender_ip, LOG_ACK_PORT, LOG_CMLT);

        }

        else if(cmd->op_type==DATA_COMMIT && recv_data_type==CMD_INFO)
            cau_server_commit(cmd);

        else if(cmd->op_type==CMD_MVMNT && recv_data_type==CMD_INFO)
            cau_send_mvm_data(cmd);

        else if(td->op_type==DATA_MVMNT && recv_data_type==UPDT_DATA)
            cau_write_hot_data(td);        

        close(connfd);

    }

    free(td);
    free(recv_buff);
    free(ack);
    free(cmd);
    free(recv_head);
    close(server_socket);

    return 0;

}
