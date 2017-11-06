CC = gcc
CFLAGS = -g -Wall -O2 -lm -pthread
Jerasure_dir = Jerasure

default:
	$(CC) $(CFLAGS) -c common.c -o common.o
	$(CC) $(CFLAGS) -o cau_client cau_client.c $(Jerasure_dir)/galois.o $(Jerasure_dir)/reed_sol.o $(Jerasure_dir)/jerasure.o common.o
	$(CC) $(CFLAGS) -o cau_server cau_server.c $(Jerasure_dir)/galois.o $(Jerasure_dir)/reed_sol.o $(Jerasure_dir)/jerasure.o common.o
	$(CC) $(CFLAGS) -o parix_client parix_client.c $(Jerasure_dir)/galois.o $(Jerasure_dir)/reed_sol.o $(Jerasure_dir)/jerasure.o common.o
	$(CC) $(CFLAGS) -o parix_server parix_server.c $(Jerasure_dir)/galois.o $(Jerasure_dir)/reed_sol.o $(Jerasure_dir)/jerasure.o common.o
	$(CC) $(CFLAGS) -o gen_chunk_distrbn gen_chunk_distrbn.c common.o
