#!/bin/bash   
  #/echo Testing before running mpiexec
   #echo CLASSPATH=$CLASSPATH
   #echo There are $# arguments to $0: $*
   #echo first argument: $1
   #echo second argument: $2
   #echo third argument: $3
   #echo here they are again: $@
   pwd
   ls -l
   echo CLASSPATH=$CLASSPATH
   echo LD=$LD_LIBRARY_PATH
   echo PATH=$PATH
   echo ==========Debug Done==========
   $@
#
