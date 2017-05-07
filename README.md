MPI YARN
===============

A lightweight application to launch MPI on YARN cluster

Usage:
======

$ bin/hadoop fs -copyFromLocal simple-yarn-app-1.0-SNAPSHOT.jar /apps/simple/simple-yarn-app-1.0-SNAPSHOT.jar

$ bin/hadoop jar simple-yarn-app-1.0-SNAPSHOT.jar com.hortonworks.simpleyarnapp.Client /bin/date 2 /apps/simple/simple-yarn-app-1.0-SNAPSHOT.jar
    
