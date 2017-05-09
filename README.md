MPI-YARN
===============

A robust and lightweight application to launch MPI on YARN cluster

## Introduction

MPI is widely used in HPC (High Performance Computing) community, which is often played as a key role in distributed computing program. Supercomputers often deploy slurm-like scheduler to manage computing resources. While traditional slurm-like scheduler is well supported in MPI, it does not support heterogeneous fine-grained scheduler like YARN directly. Sometimes we have to run MPI programs in data centers deploying YARN, as it is widely used in commercial data centers. MPI-YARN focuses on such requirement and provides support for running MPI programs in YARN-deployed data centers.

## Compile

### Fetch source code

```bash
# clone source code of MPI-YARN
git clone https://github.com/fma-cloud/mpi-yarn

# fetch submodules
git submodule update
```



### Compile hydra launcher on target system

```bash
# switch to hydra_pm
cd hydra_pm

# compile
./configure
make
```

After successfully done this step, there are at least two binary executables which is required in next steps, they are **mpiexec.hydra** and **hydra_pmi_proxy** respectively.



## Usage

### Free Mode



### Group Mode

