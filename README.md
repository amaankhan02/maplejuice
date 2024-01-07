# MapleJuice
MapleJuice is a parallel cloud computing framework written in Go, bearing similarities to Hadoop MapReduce. Features of the project include:
* **Gossip-style heartbeating failure detector** to ensure fault tolerance to up to 3 simultaneous failures. 
  * In addition to gossip-style heartbeating, we've implemented Gossip-style heartbeating with _Suspicion_ (similar to the suspicion mechanism in [SWIM](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf), but without any pinging) 
* **Simple Distributed File System (SDFS)** similar to Hadoop Distributed File System (HDFS)
    * SDFS is a flat-file system
    * Allowed file operations include 
      * `put local_filename sdfs_filename`
      * `get sdfs_filename local_filename`
      * `delete sdfs_filename`
      * `ls sdfs_filename` - list all machine addresses where `sdfs_filename` is stored
      * `store` - list all files currently being stored at the current machine
    * Allows at most one client to write a file at a time, but allows up to 2 clients to read a file simultaneously. It does not allow a writer and reader simultaneously. 
* **MapleJuice** similar to MapReduce. 
  * MapleJuice is built on top of the failure detector and SDFS. 
  * Maple is similar to the Map phase and Juice is similar to the Reduce phase. Given Maple & Juice executables written in Go, the framework will execute the job(s) parallely among the machines in the cluster and save it's outputs to SDFS.
* **SQL Engine** - A SQL Engine with implementation for Filter and Join queries that can run on CSV datasets. Implemented using MapleJuice under the hood. 

More information on the architecture for [MapleJuice](https://github.com/amaankhan02/maplejuice/tree/main?tab=readme-ov-file#maplejuice-architecture), [SDFS](https://github.com/amaankhan02/maplejuice/tree/main?tab=readme-ov-file#maplejuice-architecture), [Failure Detector](https://github.com/amaankhan02/maplejuice/tree/main?tab=readme-ov-file#maplejuice-architecture), and [SQL Engine](https://github.com/amaankhan02/maplejuice/tree/main?tab=readme-ov-file#sql-engine-architecture) can be found below after the program details.

## Program Details
### Build Instruction
* [Go](https://go.dev/) must be installed with at least version `1.19`
* Run `make` from the root directory of this project, 
  which will create an executable `app` in the root directory of the project. This is the main
executable that will be ran on each VM. `make` will also create binary executables
for all the maple, juice, and hadoop map/reduce executables in the `bin` folder.


### Command Line Arguments for `./app` 
* `./app` is the main executable that will run the MapleJuice program with the failure detector, Simple Distributed File System, and SQL Engine. `./app` has the following command line arguments: 
  * `-f` **[REQUIRED]**
    * Pass the name of the log file after `-f`. If the file does not exist, it will create it. If
      if it already exists, it will truncate the file and overwrite it
    * The program will log information to this log file, which can be used for debugging any issues.
  * `-g`
    * **default value:** `config.T_GOSSIP`. That is, if not provided, the default value is whatever the variable
    `T_GOSSIP` is set to in `internal/config/config.go`
    * Represents `T_GOSSIP` (the gossip period, i.e., the number of seconds between gossip heartbeats).
    * Represented in milliseconds
    * type: integer

### Commands
* You can run the following commands from any machine once the program has started:
    * `list_mem`
        * Lists the membership list
    * `list_self`
        * List self's id
    * `leave`
        * Voluntarily leave the group
    * `enable suspicion`
        * Enables gossip + suspicion mode
    * `disable suspicion`
        * Disables gossip + suspicion mode
        * Will run in gossip mode
    * `mode`
        * Lists current gossip mode   
    * `put [local filename] [sdfs filename]`
      * puts file from local directory to the SDFS 
    * `get [sdfs filename] [local filename]`
      * fetches file from SDFS to local directory
    * `delete [sdfs filename]`
      * delete from SDFS
    * `store`
      * list the set of SDFS files stored/replicated on current machine
    * `ls [sdfs filename]`
      * list all VM addresses where the sdfs file is currently replicated
    * `acknowledgement`
      * display all acknowledgements received to this client machine for every get/put operation
      that was made from this client machine
    * `multiread [sdfs filename] [local filename] VMi VMj ... VMk`
      * launches a GET/read operation from VMs i to k, inclusive simultaneously for the
      sdfs filename
    * `maple [maple_exe] [num_maples] [sdfs_intermediate_filename_prefix] [sdfs_src_directory]`
      * launches a maple job
    * `juice [juice_exe] [num_juices] [sdfs_intermediate_filename_prefix] [sdfs_dest_filename] delete_input={0,1}`
      * launches a juice job
    * `SELECT ALL FROM D1 WHERE <REGEX>`
      * launches a SQL FILTER query on the D1 dataset (must be stored in SDFS) by running
        maple and juice jobs
    * `SELECT ALL FROM D1, D2 WHERE <CONDITION>`
      * launches a SQL JOIN query on the D1 and D2 datasets (must be stored in SDFS) by running
        maple and juice jobs

### Further Details
* The introducer & leader is always VM1. However, this can be changed in the `config.go` by changing the `INTRODUCER_LEADER_VM` variable
* The introducer/leader must always be ran first to allow other nodes to connect to it and join the group.
* Once the `main` program starts, the non-introducer nodes automatically `join` the network
  by contacting the introducer since its membership list will only contain the introducer and itself.
  You do not need to explicitly type `join` - it automatically joins once program starts.
* You can edit other configuration variables inside `config/config.go`, such as `T_FAIL`,
  `T_CLEANUP`, `FANOUT`, for the gossip failure detection, and you can also edit the newer configuration
  variables for MP3 like `SHARD_SIZE`, `REPLICA_COUNT`, `SDFS_ROOT_DIR` etc. 
* Files in the SDFS file system will be stored in the `config.SDFS_ROOT_DIR` folder

## Architecture Details

### Failure Detector Architecture
_Coming soon..._

### SDFS Architecture
_Coming soon..._

### MapleJuice Architecture
_Coming soon..._

### SQL Engine Architecture
_Coming soon..._
