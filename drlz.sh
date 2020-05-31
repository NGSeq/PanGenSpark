#!/usr/bin/env bash

CHR=$1
INPATH=$2
DRLZOUT=$2drlz
GAPLESSOUT=$2gapless
HDFSURI=$3

hdfs dfs -mkdir -p $DRLZOUT 
hdfs dfs -mkdir -p $GAPLESSOUT

i=$(printf "%02d" $CHR)
echo Submitting job with args $i $INPATH/*.${i} $DRLZOUT 

spark-submit --master yarn-client --conf spark.executor.memory=35g --conf spark.driver.memory=35g --num-executors 32 --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.port.maxRetries=100 --conf spark.broadcast.compress=false --conf spark.executor.memoryOverhead=1000 --conf spark.driver.maxResultSize=6000m --conf spark.executor.heartbeatInterval=500 --conf spark.network.timeout=1000 --class org.ngseq.pangenspark.DistributedRLZ target/panspark-0.9-jar-with-dependencies.jar /data/grch37/radix$1 /data/grch37/chr${1}.fa $INPATH/*.${i} $HDFSURI $DRLZOUT $GAPLESSOUT/chr${i}.fa 2&> log$1
