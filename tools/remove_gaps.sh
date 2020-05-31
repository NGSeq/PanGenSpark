#!/usr/bin/env bash

set -o errexit
set -o nounset

source config.sh
# This script removes the gaps from multialign files
# saves to locally and also adds them to hdfs
#HDFS_PREFIX=hdfs://m1.novalocal/user/root/PG

# hdfs files that should have gaps removed
HDFS_MSA=$1
# hdfs path where to save new files
HDFS_PLAIN=$2
# local path where to save new files
TMP=$3

GAP_POS_PY="components/sam_to_pos/scripts/gap_positions.py"

mkdir -p $TMP
hdfs dfs -mkdir -p $HDFS_PLAIN

SIZE=$( hdfs dfs -ls $HDFS_MSA | wc -l )

task() {
    FN=$1
    TP=$2
    GAP_POS_PY=$3
    BASE=$( basename $FN )
    NAME=${TP}/${BASE}
    echo "creating ${NAME}"
    hdfs dfs -get ${filename} msa/${BASE}
    cat msa/${BASE}| tr -d 'N' > ${NAME}.plain
    hdfs dfs -put ${NAME}.plain $HDFS_PLAIN
    python $GAP_POS_PY msa/${BASE} > ${NAME}.gap_positions
}

N=${PAR_PROCESSES}
open_sem $N
# list files in hdfs and remove gaps
start=`date +%s`
for filename in `hdfs dfs -ls $HDFS_MSA | awk '{print $NF}' | tr '\n' ' '`
do
    echo $filename
    run_with_lock task $filename $TMP $GAP_POS_PY 
done
echo "Processing finished"

end=`date +%s`
runtime=$((end-start))
echo "plain: ${runtime}" >> $LOG_FILE
