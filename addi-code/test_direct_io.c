#define _GNU_SOURCE

#include <stdio.h>
#include <sys/types.h> 
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/stat.h> 
#include <stdlib.h>

#define chunk_size 4

long RSHash(char* str, unsigned int len)
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


//generate a random string with the size of len
void gene_radm_buff(char* buff, int len){

    int i;
	
    char alphanum[]="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    for(i=0; i<chunk_size; i++)
        buff[i]=alphanum[rand()%(sizeof(alphanum)-1)];

}


int main(){

	//create a file 
	int fd; 
	int i;
	int read_hash;
	int write_hash;
	
	fd=open("test_direct_io_file", O_CREAT, 0644);
	close(fd);

	//write the data into the 
	char write_buffer[chunk_size];
	char* read_buffer;

    posix_memalign((void**)&read_buffer, getpagesize(), chunk_size);

	srand((unsigned)time(NULL));

    //write the data
    printf("===>write data:\n");
	fd=open("test_direct_io_file", O_RDWR);

	for(i=0; i<10; i++){
		
	   gene_radm_buff(write_buffer, chunk_size);
	   printf("i=%d, write_hash=%ld\n", i, RSHash(write_buffer, chunk_size));
	   lseek(fd, i*chunk_size, SEEK_SET);
	   write(fd, write_buffer, chunk_size);
	   
		}

    close(fd);

	//read the data to check if inconsistency happens by using O_DIRECT flag
	printf("<===read data using O_DIRECT:\n");
	fd=open("test_direct_io_file", O_RDONLY | O_DIRECT);

	for(i=0; i<10; i++){
		
		lseek(fd, i*chunk_size, SEEK_SET);
		read(fd, read_buffer, chunk_size);
		printf("i=%d, read_hash=%ld\n", i, RSHash(read_buffer, chunk_size));

		}

	close(fd);

	printf("<===read data without O_DIRECT:\n");
	fd=open("test_direct_io_file", O_RDONLY);

	for(i=0; i<10; i++){
		
		lseek(fd, i*chunk_size, SEEK_SET);
		read(fd, read_buffer, chunk_size);
		printf("i=%d, read_hash=%ld\n", i, RSHash(read_buffer, chunk_size));

		}

	free(read_buffer);

}

