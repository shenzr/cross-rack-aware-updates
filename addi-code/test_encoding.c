#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define data_chunks 6
#define prty_chunks 3
#define num_chunks  data_chunks+prty_chunks
#define chunk_size 3*sizeof(long) //the size of a chunk
#define num_updt_chnks 4

int* encoding_matrix;


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

void print_chunk_info(char* chunk, int w){

	int j,x;
	
	for(j=0;j<chunk_size; j+=(w/8)) { 
	printf(" ");
	for(x=0;x < w/8;x++){
		printf("%02x", (unsigned char)chunk[j+x]);
		}
	}

	printf("\n");
	
}


int obtain_encoding_coeff(int given_chunk_id, int prtyid_to_update){


    int index;

    index=given_chunk_id%data_chunks;

    return encoding_matrix[prtyid_to_update*data_chunks+index];
}

void partial_encode_data(char* data, char* pse_coded_data, int chunk_id, int updt_prty){

	//printf("encode_data works:\n");

    int ecd_cef=obtain_encoding_coeff(chunk_id,updt_prty);
	printf("encod_marix[%d][%d]=encod_coeff=%d\n", updt_prty, chunk_id%data_chunks, ecd_cef);
    galois_w08_region_multiply(data, ecd_cef, chunk_size, pse_coded_data, 0);

}

//for each receive data, perform XOR operations to them
void aggregate_data(char* data_delta, int num_recv_chnks, char* ped){

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

        //printf("res_addr=%p, addrA=%p\n", res, addrA);
        bitwiseXor(res, addrA, ped+i*chunk_size, chunk_size);
	    //printf("res=%x, addrA_addr=%x\n", tmp, addrA);
        tmp=addrA;
        addrA=res;
        res=tmp;
    }
	
    memcpy(data_delta, addrA, chunk_size);

	//printf("out aggregated_data:\n");

}


void print_array(int row, int col, int *array){

  int i,j; 

  for(i=0; i<row; i++){

	for(j=0; j<col; j++)
		printf("%d ", array[i*col+j]);

	printf("\n");

  	}
}


void print_data_and_coding(int k, int m, int w, int size, char **data, char **coding) {
	
  int i, j, x;
  int n, sp;
  long l;

  if(k > m) n = k;
  else n = m;
  sp = size * 2 + size/(w/8) + 8;

  printf("%-*sCoding\n", sp, "Data");
  for(i = 0; i < n; i++) {
	  if(i < k) {
		  printf("D%-2d:", i);
		  for(j=0;j< size; j+=(w/8)) { 
			  printf(" ");
			  for(x=0;x < w/8;x++){
				printf("%02x", (unsigned char)data[i][j+x]);
			  }
		  }
		  printf("    ");
	  }
	  else printf("%*s", sp, "");
	  if(i < m) {
		  printf("C%-2d:", i);
		  for(j=0;j< size; j+=(w/8)) { 
			  printf(" ");
			  for(x=0;x < w/8;x++){
				printf("%02x", (unsigned char)coding[i][j+x]);
			  }
		  }
	  }
	  printf("\n");
  }
	printf("\n");
}


void encoding_indivi(int w, char** raw_data, int prty_id){

    int j,x;
	int i;

	printf("generating %d-th parity:\n", prty_id);
	
	char* code_data=(char*)malloc(chunk_size);
	char* pse_data_set=(char*)malloc(chunk_size*data_chunks);

	for(i=0; i<data_chunks; i++)
		partial_encode_data(raw_data[i], pse_data_set+i*chunk_size, i, prty_id);

	//print the coded chunk 
	for(i=0; i<data_chunks; i++){
       for(j=0;j< chunk_size; j+=(w/8)) { 
	      printf(" ");
	      for(x=0;x < w/8;x++){
		  printf("%02x", (unsigned char)pse_data_set[i*chunk_size+(j+x)]);
		  }
	  }
	  printf("\n");
	}
	
	//aggregate the data
	memcpy(code_data, pse_data_set, chunk_size);
	aggregate_data(code_data, data_chunks-1, pse_data_set+chunk_size);

	//print the coded chunk 
	printf("the %d-th coded chunk:\n", prty_id);
    for(j=0;j<chunk_size; j+=(w/8)) { 
	   printf(" ");
	   for(x=0;x < w/8;x++){
		 printf("%02x", (unsigned char)code_data[j+x]);
		}
	}

	printf("\n");

	free(pse_data_set);
	free(code_data);

}


void jerasure_encode_data(char** data, char** coding, int w){


    /* original encoding in the jerasure library */
	jerasure_matrix_encode(data_chunks, prty_chunks, w, encoding_matrix, data, coding, chunk_size);

	printf("Data and Parity:\n");
	print_data_and_coding(data_chunks, prty_chunks, w, chunk_size, data, coding);


}


int main(){

	int i;
	int j;
	int x;
	int coeff;
	int w=8;

	encoding_matrix=reed_sol_vandermonde_coding_matrix(data_chunks, prty_chunks, w);

	printf("encoding_matrix:\n");
	print_array(prty_chunks, data_chunks, encoding_matrix);

	//generate the data chunks 
	char** data;
	char** coding; 
	char* temp_chunk;
	char* updt_data;

	//init data and coding
	data=(char**)malloc(sizeof(char*)*data_chunks);
	coding=(char**)malloc(sizeof(char*)*prty_chunks);

	updt_data=(char*)malloc(chunk_size);

	for(i=0; i<data_chunks; i++){

		temp_chunk=(char*)malloc(chunk_size);

		for(j=0; j<chunk_size; j++)
			temp_chunk[j]='a';

		data[i]=temp_chunk;

		}

	for(i=0; i<prty_chunks; i++)
		coding[i]=(char*)malloc(chunk_size);

	jerasure_encode_data(data, coding, w);
	 
	//using partial encoding
	char** raw_data=(char**)malloc(sizeof(char*)*data_chunks);

	for(i=0; i<data_chunks; i++)
		raw_data[i]=(char*)malloc(chunk_size);

	for(j=0; j<data_chunks; j++)
	    for(i=0; i<chunk_size; i++)
		   raw_data[j][i]='a';

    //perform partial encoding
	for(i=0; i<prty_chunks; i++)
		encoding_indivi(w, raw_data, i);

	printf("update four chunks:\n");
	//perform update 
	for(i=0; i<chunk_size; i++)
		updt_data[i]='b';

	//calculate delta 
	char* delta=(char*)malloc(chunk_size);
	char* pse=(char*)malloc(chunk_size);
	char** prty_delta=(char**)malloc(sizeof(char*)*prty_chunks);

	char* temp_prty_delta=(char*)malloc(chunk_size);

	for(i=0; i<prty_chunks; i++)
		prty_delta[i]=(char*)malloc(chunk_size);

    //calculate the data delta
	bitwiseXor(delta, updt_data, data[0], chunk_size);

    printf("data:\n");
	print_chunk_info(data[0], w);

    printf("updt_data:\n");
	print_chunk_info(updt_data, w);

    printf("delta:\n");
	print_chunk_info(delta, w);


	//update each parity chunk
	for(i=0; i<prty_chunks; i++){

		for(j=0; j<num_updt_chnks; j++){

			//calculate parity delta 
			partial_encode_data(delta, pse, j, i);

			printf("%d-th data-chunk:\n", j);
			printf("---%d-th parity-chunk:\n", i);
			print_chunk_info(pse, w);

			if(j==0){

				memcpy(prty_delta[i], pse, chunk_size);
				continue;

				}

			memcpy(temp_prty_delta, prty_delta[i], chunk_size);

			//update the new parity
			bitwiseXor(prty_delta[i], pse, temp_prty_delta, chunk_size);

			}
		}

	//print the new parity info
	for(i=0; i<prty_chunks; i++){

		printf("%d-th new parity:\n", i);
		print_chunk_info(prty_delta[i], w);

		}

	for(i=0; i<data_chunks; i++)
		free(data[i]);
	free(data);

	for(i=0; i<prty_chunks; i++)
		free(coding[i]);
	free(coding);

	for(i=0; i<data_chunks; i++)
		free(raw_data[i]);
	free(raw_data);

	for(i=0; i<prty_chunks; i++)
		free(prty_delta[i]);
	free(prty_delta);

	free(delta);
	free(updt_data);
	free(pse);
	free(temp_prty_delta);

	return 0;
}

