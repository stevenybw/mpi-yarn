MPI-YARN
===============

A robust and lightweight application to launch MPI on YARN cluster

## Introduction

MPI is widely used in HPC (High Performance Computing) community, which is often played as a key role in distributed computing program. Supercomputers often deploy slurm-like scheduler to manage computing resources. While traditional slurm-like scheduler is well supported in MPI, it does not support heterogeneous fine-grained scheduler like YARN directly. Sometimes we have to run MPI programs in data centers deploying YARN, as it is widely used in commercial data centers. MPI-YARN focuses on such requirement and provides support for running MPI programs in YARN-deployed data centers.

## Compile

### Dependency

We assume the following dependency:

1. autoconf, automake, libtool
2. maven

### Fetch source code

```bash
# clone source code of MPI-YARN
git clone --recursive https://github.com/fma-cloud/mpi-yarn
```



### Compiling mpi-yarn bytecode

```bash
# compile mpi-yarn using maven
mvn clean package
```

You will see **./target/mpi-yarn-${VERSION}.jar**, which contains the client and application master on YARN. Next, we will compile the dependency hydra-launcher.

### Compile hydra launcher on target system

```bash
# install dependency: hydra_pm
cd hydra-pm

# compile
./autogen.sh
./configure
make
```

After successfully done this step, there are at least two binary executables which is required in next steps, they are **mpiexec.hydra** and **hydra_pmi_proxy** respectively.

## Configure

**yrun** is a python-based mpiexec-like wrapper to simplify the submission. Before using mpi-yarn, you need to modify some arguments in yrun. 

```python
QUEUE_NAME = "default" # change to your queue name
HADOOP_PREFIX = "/Users/ybw/local/hadoop-2.6.5" # change to absolute path of hadoop
HDFS_PREFIX = "hdfs://localhost:9000" # change to your HDFS rpc port
MPI_YARN_PREFIX = "/Users/ybw/OpenSourceCode/mpi_yarn" # change to absolute path of mpi-yarn
MPI_YARN_JAR_NAME = "/target/mpi-yarn-1.0.0.jar" # no need to change
CONTAINER_MEMORY_MB = "2048" # memory in MB per container
```

## Usage

### Free Mode



### Group Mode

