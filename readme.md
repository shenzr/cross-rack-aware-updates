This source code is a simple implementation to evaluate the delta-based update in erasure coded data centers. Please refer to the paper: 
*Cross-Rack-Aware Updates in Erasure-Coded Data Centers* (in Proceedings of 47th International Conference on Parallel Processing (**ICPP'18**)) for more technical details. 

Before running the code, please take the following steps first: 

- ensure that the necessary libraries and compile tools are install, including gcc and make. 

- change the default configurations in "config.h", including the erasure code you wish to use, and data center architecutre (e.g., the number of racks, and the number of nodes in each rack). Notice that we currently assume that all racks are composed of the same number of nodes. 

- please generate a big file named "data_file" which will be used for simulating disk reads and writes. In our test, we generally set its size as 60GB. 

- If you wish to deploy the code onto Amazon EC2, please do the following two things. 
   * carefully specify the "security group" by only allowing the communications among the VMs used in the test. We have ever encountered unexpected connections (may be from other VMs), which will definitely affect the running status of evaluations. 


   * Each VM in Amazon EC2 has two ip addresses, the public ip and the inner ip. You have to fill them in the common.c. 

- In the code, we need to read the ip address from the NIC. In our test, the NIC in our testbed is "enp0s31f6" (see the function "GetLocalIp" in common.c). If your machine is equipped with a different NIC, please replace "enp0s31f6" with it. 

An example of running CAU codes: 

- make: this step will generate the executable files 

- copy the executable files with the suffix of "\_mds" and the "chunk\_map" file to the MDS. 

- copy the executable files with the suffix of "\_server" to storage nodes. 

- run "gen\_chunk_distribn" on the metadata server (MDS), which will generate the mapping information between the logical chunk and the associated storage node.  The mapping information will be recorded in a file named "chunk\_map" in MDS. The MDS will read it for chunk addressing. Notice that the mapping information is generated based on the selected erasure coding and the system architecture specified in "config.h". 

- run the executable files with the suffix of "\_mds" (e.g., cau\_mds) on MDS and those with the suffix "\_server" (e.g., cau\_server) on storage nodes. 

- run the executable file with the suffix "\_client" on the client side with the trace file to evaluate. 

If you have any question, please feel free to contact me (zhirong.shen2601@gmail.com). 
