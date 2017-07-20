MPI-YARN
===============

A robust and lightweight application to launch MPI on YARN cluster

## Introduction

MPI is widely used in HPC (High Performance Computing) community, which is often played as a key role in distributed computing program. Supercomputers often deploy slurm-like scheduler to manage computing resources. While traditional slurm-like scheduler is well supported in MPI, it does not support heterogeneous fine-grained scheduler like YARN directly. Sometimes we have to run MPI programs in data centers deploying YARN, as it is widely used in commercial data centers. MPI-YARN focuses on such requirement and provides support for running MPI programs in YARN-deployed data centers.

## 0. What you need to know before using mpi-yarn
 - You need maven for compiling mpi-yarn
 - You need autoconf, automake, and libtool for compiling hydra
 - We have only tested MPI programs compiled by MPICH. We cannot promise that programs compiled through other MPI implementations will work with this tool.

## 1. mpi-yarn
### 1.1 Fetch source code

```bash
# clone source code of MPI-YARN
git clone --recursive https://github.com/fma-cloud/mpi-yarn
```

### 1.2 Compiling mpi-yarn bytecode

```bash
# compile mpi-yarn using maven
mvn clean package
```
*You are expected you already have maven installed. 

You will see **./target/mpi-yarn-${VERSION}.jar**, which contains the client and application master on YARN. Next, you will have to compile the dependency—hydra-launcher, or use the pre-compiled version that we have provided. 

## 2. hydra launcher
### 2.1 Getting a set of working hydra executables. 
#### Option one:  pre-compiled binary files
There should be already a pre-compiled version of hydra with in the folder hydra-pm. We have tested the pre-compiled version on the following systems:

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

There will should several binary execuatables in in the folder, in which there are at least two required in next steps. They are **mpiexec.hydra** and **hydra_pmi_proxy** respectively.

## 3. Configuration

For mpi-yrun to be more user-friendly, we also provide **yrun** , a python-based mpiexec-like wrapper to simplify the submission process. Before using this script, you need to modify some of the arguments in yrun. 

```python
QUEUE_NAME = "default" # change to your queue name
HADOOP_PREFIX = "/Users/ybw/local/hadoop-2.6.5" # change to absolute path of hadoop
HDFS_PREFIX = "hdfs://localhost:9000" # change to your HDFS rpc port
MPI_YARN_PREFIX = "/Users/ybw/OpenSourceCode/mpi_yarn" # change to absolute path of mpi-yarn
MPI_YARN_JAR_NAME = "/target/mpi-yarn-1.0.0.jar" # no need to change(In the future you might have to change the version number)
CONTAINER_MEMORY_MB = "2048" # memory in MB per container
```

# Using mpi-yarn
### 4.1 Submitting jobs through the wrapper
```bash
./yrun [optional argument no.1][optional argument no.2][optional argument no.3]…[optional argument no. N] [location of the MPI executable and its argument] 
```
**yrun** takes the following optional arguments: 
|  Argument | usage  |
|---|---|
|-h   |provides a list of the arguments   |
|-n [int]|How many process should this job to start|
|-N  [int] |How many nodes should this job to use. Use in conjunction with -ppn   |
|-ppn [int]   |How many process should run on a single node. Use in conjunction with -N   |
| -so [path to .so files no.1],etc.   |.so files that the MPI executables needs in order for it to run on the nodes. Those files will be uploaded to every node. If the environment is the same on all the machines, there shouldn't be a need to use this.  |
|-env [environment variable name no.1], etc.|environment variable names to pass|
|-m [int]|The amount of memory that each container will take. The default value is set to be 2048 mb in **yrun**, but you can overwrite that value with this argument|
|-sf [localpath:remotepath],etc.|If there are small input files/configuration files that the MPI executable relies on, you can use this argument to have that file(s) be uploaded to every node. Note: the remote path should be a relative path in relation to the MPI executable, and it cannot be outside of the temp folder that yarn creates. Larger input files should be uploaded onto HDFS, and the MPI executable should be altered to take the input file directly from HDFS.Currently this argument does not support uploading by folders|


----------
For example, if I want to run **ls** on four node while uploading a small text file **config.txt**, this would be the command that I run:

```bash
./yrun -N 4 -ppn 1 -sf /home/TJGuo/config.txt:config/config.txt /bin/ls config/
```
The output of this should be
```bash
config.txt
```

