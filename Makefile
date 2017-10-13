CC = gcc
CFLAGS = -g -Wall -O2 -lm -pthread

default:

	$(CC) $(CFLAGS) -o client_side client_side.c 
	$(CC) $(CFLAGS) -o server_side server_side.c 