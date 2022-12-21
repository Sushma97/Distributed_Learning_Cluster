# IDunno - Distributed Learning Cluster

IDunno is a compute framework that can scale AI/Machine learning workloads. It is scalable (any number of models can be added to the system) and fault tolerant (no query/data is lost) upto 3 simultaneous failures. It uses SWIM style failure detection and ring style leader election algorithm. To store test data and results it uses a distributed flat file system (SDFS). 

IDunno has two phases, training and inference. In training phase it loads all the pre-defined models, in Inference phase it performs query on the appropriate model. VMs are reassigned/balanced based on the query rate between jobs. 

## Design
IDunno system runs on top of distributed file system and MP2 full membership list. It is based on a simple implementation of ray.io. At each node, we keep the 5 previous threads (TCP listener, UDP listener, Ping/ACK thread, leader election thread and command thread), with the addition of worker thread which polls coordinator for queries. All messages flow through a single port, and are sent to the right handler based on type (differentiated between IDunno, file server, membership info, and coordinator). The introducer is now at a fixed location backed by a file, as per the recommendations. IDunno commands are relayed to the coordinator by the command handler thread. IDunno messages are communicated via TCP.
Look at report.pdf for detailed information on design

# Instructions

- STEP 1: Run the introducer
    * ssh into the machine ```ssh <NETID>@fa22-cs425-ggXX.cs.illinois.edu```
    * Clone the project ```https://gitlab.engr.illinois.edu/sushmam3/mp4_idunno.git```
    * Build the project ```mvn -DskipTests package``` (the tests pass, but it's faster to build without them)
    * cd to scripts folder and run the introducer.sh ```./introducer.sh <port-number>```

- STEP 2: Run the python Flask application
    * cd to python_script folder and run the model.py file. This exposes the RESTful APIs for model inference and training on local host port 8080

- STEP 3: Run the member (this will run the worker, file server and leader election as well, one of the members will be chosen as coordinator)
    * ssh into the machine ```ssh <NETID>@fa22-cs425-ggXX.cs.illinois.edu```
    * Clone the project ```https://gitlab.engr.illinois.edu/sushmam3/mp4_idunno```
    * Build the project ```mvn -DskipTests package```
    * cd to scripts folder and run the ```member.sh ./member.sh <port-number>```
        On the command prompt, these are the following options.
        * ```populate-test-data datapath``` - puts test data into SDFS from datapath
        * ```train-start``` - Trains and Loads the model in all workers, making it ready for inference
        * ```add-job modelname``` - Adds the job into IDunno system
        * ```set-batch-size modelname size``` - Set the query batch size for given model.
        * ```start-job modelname``` - Start the job for the given model. Coordinator reschedules VMs based on job query rate.
        * ```stop-job modelname``` - Stop the job for the given model
        * ```get-job-stats``` - Display each running job, statistics such as total number of queries processed, moving average query rate
        * ```get-job-assigned-VMs``` - Display the VMs assigned for each running job. 
        * ```join``` - join the network 
        * ```leave``` - leave the network
        * ```list_mem``` - Display the membership list
        * ```list_self``` - Display self information
        * ```put localfilename sdfsfilename``` - Put file into SDFS from local dir. If file exists it will create a new version.
        * ```get sdfsfilename localfilename``` - Get file from SDFS into local dir
        * ```delete sdfsfilename``` - Delete the file from SDFS
        * ```ls sdfsfilename``` - list all machine (VM) addresses where this file is currently
          being stored
        * ```store``` - At any machine, list all files currently being stored at this
          machine
        * ```get-versions sdfsfilename num-versions localfilename``` - gets all the last num-versions
          versions of the file into the localfilename (uses delimiters to mark out
          versions). 

