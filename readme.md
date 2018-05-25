This source code is a simple implementation to evaluate the delta-based update in erasure coded data centers. Please refer to the paper: 
*Cross-Rack-Aware Updates in Erasure-Coded Data Centers* (in Proceedings of 47th International Conference on Parallel Processing (**ICPP'18**)) for more technical details. 

Before running the code, please take the following steps first: 

- ensure that the necessary libraries and compile tools are installed, including gcc and make. 

- change the default configurations in "config.h", including the erasure code you wish to use, and the data center architecutre (e.g., the number of racks, and the number of nodes in each rack). Notice that we currently assume that all racks are composed of the same number of nodes. 

- please generate a big file named "data\_file" which will be used for simulating disk reads and writes. In our test, we generally set its size as 60GB. 

- In the code, we need to read the ip address from the NIC. The NIC in our testbed is "enp0s31f6" (see the NIC in common.c). If your machine is equipped with a different NIC, please replace "enp0s31f6" with it. 

- fill the ip addresses of the nodes you use (including storage nodes, MDS, and client) in common.c, where "inner\_ip\" denotes the ip address read from NIC, and "public\_ip" denotes the ip address used in socket communications. 

- If you wish to deploy the code onto Amazon EC2, please do the following two things. 
   * carefully specify the "security group" by only allowing the communications among the VMs used in the test. We have ever encountered unexpected connections (may be from other VMs), which will definitely affect the running status of evaluations. 

   * Each VM in Amazon EC2 has two ip addresses, the public ip and the inner ip. You have to fill them in common.c.

- In our evaluation, we use a gateway server to mimic cross-rack data transters in a local cluster (see our paper for more details). If you wish to do this, please do the following two things: 

   * set the GTWY\_OPEN as 1 in config.h

   * set the gateway\_ip (the socket ip for communication) and the gateway\_local_ip (the NIC ip) in common.c 

An example of running CAU codes: 

- make: this step will generate the executable files 

- run "gen\_chunk_distribn" on the metadata server (MDS), which will generate the mapping information between the logical chunks and the associated storage nodes. The mapping information will be recorded in a file named "chunk\_map" in MDS. The MDS will read it for chunk addressing. Notice that the mapping information is generated based on the selected erasure coding and the data center architecture specified in "config.h". 

- copy the executable files with the suffix of "\_mds" and the "chunk\_map" file to the MDS. 

- copy the executable files with the suffix of "\_server" to storage nodes (including the gateway server if enabled). 

- run the executable files with the suffix of "\_mds" (e.g., cau\_mds) on MDS and those with the suffix "\_server" (e.g., cau\_server) on storage nodes. 

- run the executable file with the suffix "\_client" on the client side with the trace file to evaluate (e.g., ./cau\_client wdev\_1.csv). Some example traces are included in "example-traces"

If you have any question, please feel free to contact me (zhirong.shen2601@gmail.com). 
