#!/usr/bin/env bash
set -e 
set -o pipefail

HDFS_MSA=$1
LOCAL_MSA=$2
LOCAL_OUTPUT_FOLDER=$3
TMP=$4

echo "create.sh" ${HDFS_MSA} ${LOCAL_MSA} ${LOCAL_OUTPUT_FOLDER} ${TMP}

source config.sh

JAR_PATH="../../target/panspark-0.9-jar-with-dependencies.jar"

start=`date +%s`
#spark-submit --master yarn --deploy-mode client --conf spark.executor.memory=16g --conf spark.driver.extraJavaOptions="-Dlog4jspark.root.logger=WARN,console"\

drlz(){
i=$(printf "%02d" $1)
echo Submitting job with args $i

spark-submit --master yarn --deploy-mode client --num-executors 50 --conf spark.executor.memory=35g --conf spark.driver.memory=35g \
--conf spark.broadcast.compress=false --conf spark.executor.memoryOverhead=1000 --conf spark.port.maxRetries=100 \
--conf spark.driver.maxResultSize=6000m --conf spark.executor.heartbeatInterval=500 --conf spark.network.timeout=1000 \
--conf spark.yarn.executor.memoryOverhead=1000 --class org.ngseq.panquery.DistributedRLZ2 \
${JAR_PATH} /media/msa/hg19/UCASE/radix$1 /media/msa/hg19/UCASE/chr${1}_uc.fa ${HDFS_MSA}/*.$i.gz hdfs://node-1.novalocal:8020 drlzout

}

seq 1 22 | parallel -j3 drlz()

end=`date +%s`
runtime=$((end-start))
echo "rlz: ${runtime}" >> $LOG_FILE

# copy local

# create virtualenv
#python create_fasta.py -i ${TMP} -p ${TMP} -o ${LOCAL_OUTPUT_FOLDER}/pangen.fa
#truncate -s -1 ${LOCAL_OUTPUT_FOLDER}/pangen.fa

# create index

ORIG=$( pwd )

# change directory to run chic

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

hdfs dfs -getmerge drlzout merged.lz
PG_FASTA="${LOCAL_OUTPUT_FOLDER}/pangen.fa"
# last parameter should be over 100 (read size)
# save everything still to project root
start=`date +%s`
${DIR}/chic_index --threads=${THREADS} --kernel=BOWTIE2 --lz-input-file=${ORIG}/merged.lz ${PG_FASTA}  ${MATCH_LEN} 2>&1 | tee ${LOCAL_OUTPUT_FOLDER}/index.log
end=`date +%s`
runtime=$((end-start))
echo "index: ${runtime}" >> $LOG_FILE
