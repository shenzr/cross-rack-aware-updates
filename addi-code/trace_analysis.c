#define _GNU_SOURCE


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <math.h>

#include <net/if.h>
#include <netinet/in.h>
#include <net/if_arp.h>
#include <arpa/inet.h>

#define num_trace_anlyzed_pertime 4


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


//read a trace
void trace_analysis_process(char* trace_name){

    //read the data from csv file
    FILE *fp;

    if((fp=fopen(trace_name,"r"))==NULL){
        printf("open file failed\n");
        exit(0);
    }


    char operation[150];
    char time_stamp[50];
    char workload_name[10];
    char volumn_id[5];
    char op_type[10];
    char offset[20];
    char size[10];
    char usetime[10];
    char divider=',';

    int i;
    int op_size;
    int ret;


    int read_count;
    int write_count;
    double aver_write_size;
    double deviation;
    double write_size;


    long long *size_int;
    long long *offset_int;
    long long a,b;
    a=0LL;
    b=0LL;
    offset_int=&a;
    size_int=&b;

    read_count=0;
    write_count=0;
    aver_write_size=0;
    deviation=0;

    struct timeval bg_tm, ed_tm; 

    while(fgets(operation, sizeof(operation), fp)) {

        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
        new_strtok(operation,divider,usetime);

        if(strcmp(op_type, "Read")==0){
            read_count++;
            continue;
        }

        else if(strcmp(op_type, "Write")==0)
            write_count++;

        //for the write operation, calculate the average write size
        trnsfm_char_to_int(size, size_int);

        aver_write_size+=(*size_int)*1.0/1024;

    }

    aver_write_size/=write_count;

    //set fp to the top of the file and calculate the standard deviation
    rewind(fp);

    while(fgets(operation, sizeof(operation), fp)){

        new_strtok(operation,divider,time_stamp);
        new_strtok(operation,divider,workload_name);
        new_strtok(operation,divider,volumn_id);
        new_strtok(operation,divider,op_type);
        new_strtok(operation,divider,offset);
        new_strtok(operation,divider, size);
        new_strtok(operation,divider,usetime);

        if(strcmp(op_type, "Read")==0)
            continue;

        //get the write size
        trnsfm_char_to_int(size, size_int);

        write_size=(double)((*size_int)*1.0/1024);

        deviation+=pow((write_size-aver_write_size),2);

    }

    deviation=sqrt(deviation/write_count);

    printf("++++ Trace: %s, Write_Count=%d, Write_Ratio=%lf, Aver_Write_Size=%lf, Deviation=%lf +++++++\n", trace_name, write_count, write_count*1.0/(write_count+read_count), aver_write_size, deviation);

    fclose(fp);

}


int main(int argc, char** argv){

    if(argc!=2){
        printf("./trace_analysis trace_name1 trace_name2 tace_name3 trace_name4!\n");
        exit(0);
    }

    trace_analysis_process(argv[1]);

    return 0;

}

