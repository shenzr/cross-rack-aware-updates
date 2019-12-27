CC = gcc 
all: gen_chunk_map trace_analysis
gen_chunk_map: gen_chunk_map.c
	$(CC) gen_chunk_map.c -o gen_chunk_map
trace_analysis: trace_analysis.c 
	$(CC) trace_analysis.c -o trace_analysis

clean:
	rm gen_chunk_map trace_analysis cau_chunk_map*
