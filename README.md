MPI-YARN
===============

A robust and lightweight application to launch MPI on YARN cluster

## Introduction

MPI is widely used in the HPC (High Performance Computing) community, which often plays a key role in distributed computing programs. Supercomputers often deploy slurm-like scheduler to manage computing resources, and while traditional slurm-like scheduler is well supported in MPI, it does not support heterogeneous fine-grained scheduler like YARN directly. Sometimes we have to run MPI programs in data centers deploying YARN, as it is widely used in commercial data centers. MPI-YARN focuses on such requirement and provides support for running MPI programs in YARN-deployed data centers.

## 0. What you need to know before using mpi-yarn
 - You need maven for compiling mpi-yarn
 - You need autoconf, automake, and libtool for compiling hydra
 - We have only tested MPI programs compiled by MPICH. We cannot promise that programs compiled through other MPI implementations will work with this tool.

## 1. mpi-yarn
### 1.1 Fetch source code

```bash
# clone source code of MPI-YARN
git clone --recursive https://github.com/fma-cloud/mpi-yarn.git
```

### 1.2 Compile

```bash
# compile mpi-yarn using maven
mvn clean package
```
*You are expected to have maven installed. 

You will see **./target/mpi-yarn-${VERSION}.jar**, which contains the client and application master on YARN. Next, you will have to compile the dependency, the Hydra process manager, or use the pre-compiled version that we have provided. 

## 2. Hydra launcher
The application launches MPI grograms on the YARN cluster by using the Hydra process manager, thus the user will be required to acquire a set of working Hydra executables.
### 2.1 Getting a set of working Hydra executables. 
#### Option one:  pre-compiled binary files
A pre-compiled version of Hydra should be in the folder hydra-compiled. We have tested the pre-compiled executables on the following systems:

 - Ubuntu 12.04/16.04
 - CentOS 6.5
 - openSUSE 11SP3

#### Option two: Compile it yourself
If for some reason the pre-compiled version does not work with your environment, then you will have your compile it yourself. 
```bash
# install dependency: hydra_pm
cd hydra-pm

# compile
./autogen.sh
./configure
make
```
*You are expected to have autoconf, automake, and libtool installed. 

### 2.2 Check the files.

There should be several binary execuatables in the folder, of which at least two are required. They are **mpiexec.hydra** and **hydra_pmi_proxy**.

## 3. Configuration

For mpi-yrun to be more user-friendly, we also provide **yrun** , a python-based mpiexec-like wrapper to simplify the submission process. Though before using this, the user needs to modify some of the arguments in it. 

```python
QUEUE_NAME = "default" # change to your queue name
HADOOP_PREFIX = "/Users/ybw/local/hadoop-2.6.5" # change to absolute path of hadoop
HDFS_PREFIX = "hdfs://localhost:9000" # change to your HDFS rpc port
MPI_YARN_PREFIX = "/Users/ybw/OpenSourceCode/mpi_yarn" # change to absolute path of mpi-yarn
MPI_YARN_JAR_NAME = "/target/mpi-yarn-1.0.0.jar" # no need to change(In the future you might have to change the version number)
CONTAINER_MEMORY_MB = "2048" # memory in MB per container
HYDRA_PREFIX = "/hydra_pm" #location of the HYDRA executables. For examile it would be /hydra_compile if using the pre-compiled version
```
*The wrapper is written with Python 2 syntax, and currently does not support Python3

# Using mpi-yarn
### 4.1 Submitting jobs through the wrapper
```bash
./yrun [optional argument no.1][optional argument no.2][optional argument no.3]…[optional argument no. N] [location of the MPI executable and its argument] 
```

|  Argument | usage  |
|-------------|-------------|
|-h   |provides a list of the arguments   |
|-n [int]|How many process should this job to start|
|-N  [int] |How many nodes should this job to use. Use in conjunction with -ppn   |
|-ppn [int]   |How many process should run on a single node. Use in conjunction with -N   |
| -so [path to .so files no.1],etc.   |.so files that the MPI executables needs in order for it to run on the nodes. Those files will be uploaded to every node. If the environment is the same on all the machines, there shouldn't be a need to use this.  |
|-env [name=environment variable], [name=environment variable], etc. |If the MPI program requires additional environmental variables, use this argument to pass the name of the argument, and the intended value.|
|-m [int]|The amount of memory that each container will take. The default value is set to be 2048 mb in **yrun**, but you can overwrite that value with this argument|
|-sf [localpath:remotepath],etc.|If there are small input files/configuration files that the MPI executable relies on, you can use this argument to upload to every node. The remote path should be a relative path to the MPI executable, and it cannot be outside of the temp folder that YARN creates. Larger input files should be uploaded onto HDFS, and the MPI executable should be altered to take the input file directly from HDFS. Currently this argument does not support uploading by folders|
 -----------
 Here would be a example command running **ls**, using four node with one process on each node, uploading a .so file libc.so.6 which can be found at /lib/x86_64-linux-gnu/libc.so.6, setting the memory requirement to be 512mb, setting **PATH** on the nodes to be /home, and uploading a configuration file config.txt that is in the config folder:
 
 ```bash
 ./yrun -N 4 -ppn 1 -env PATH=/home -m 512 -so /lib/x86_64-linux-gnu/libc.so.6 -sf /home/TJGuo/config.txt:config/config.txt /bin/ls config/
 ```
 The output of this, without all the debug information, would be:
 ```bash
 config.txt
 config.txt
 config.txt
 config.txt
 ```
