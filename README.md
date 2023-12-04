# cs425_mp4

## Build Instruction
* Program is written in Go, so make sure you have go installed
* Once installed, from the root directory of this project, run `make`
  which will create an executable `app` in the root directory of the project. This is the main
executable that will be ran on each VM. `make` will also create binary executables
for all the maple, juice, and hadoop map/reduce executables in the `bin` folder.


## Command Line Arguments for `./app` 
* `-f` **[REQUIRED]**
    * Pass the name of the log file after `-f`. If the file does not exist, it will create it. If
      if it already exists, it will truncate the file and overwrite it
* `-g`
    * **default value:** `config.T_GOSSIP`. That is, if not provided, the default value is whatever the variable
    `T_GOSSIP` is set to in `internal/config/config.go`
    * Represents `T_GOSSIP` (the gossip period, i.e., the number of seconds between gossip heartbeats).
    * Represented in milliseconds
    * type: integer

## Details
* The introducer & leader is always VM1. However, this can be changed in the `config.go` by changing the `INTRODUCER_LEADER_VM` variable
* The introducer/leader must always be ran first to allow other nodes to connect to it and join the group.
* Once the `main` program starts, the non-introducer nodes automatically `join` the network
  by contacting the introducer since its membership list will only contain the introducer and itself.
  You do not need to explicitly type `join` - it automatically joins once program starts.
* You can edit other configuration variables inside `config/config.go`, such as `T_FAIL`,
  `T_CLEANUP`, `FANOUT`, for the gossip failure detection, and you can also edit the newer configuration
  variables for MP3 like `SHARD_SIZE`, `REPLICA_COUNT`, `SDFS_ROOT_DIR` etc. 
* Files in the SDFS file system will be stored in the `config.SDFS_ROOT_DIR` folder
* You can run the following commands in the program command line from any node once the program has started:
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
