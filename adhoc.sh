CHR=$1
PGFOLDER=$2
PGPATHLOCAL=$3
OUTPATH=$4
BLOCKS=30
DEPTH=100
HDFSURL=$5

i=$(printf "%02d" $CHR)

FULL_SEQ=$( ls ${PGPATHLOCAL}/*.$i | head -1 )
numfiles=(${PGPATHLOCAL}/*.$i)
numfiles=${#numfiles[@]}
SIZE=$( cat $FULL_SEQ | wc -m )
start=`date +%s`
spark-submit --master yarn --deploy-mode client --num-executors 32 --executor-memory 35g --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.executor.heartbeatInterval=500 --conf spark.network.timeout=1000 --conf spark.executor.memoryOverhead=1000 --conf spark.port.maxRetries=100 --conf spark.driver.memory=36g --conf spark.driver.maxResultSize=25g --class org.ngseq.pangenspark.ScoreMatrix target/panspark-0.9-jar-with-dependencies.jar pos/*.$i.pos ${PGFOLDER}/*.$i ${SIZE} $BLOCKS ${numfiles} $DEPTH $OUTPATH $HDFSURL > scorematrix$i.log

hdfs dfs -get $OUTPATH adhoc/pangen_ref_chr$i.fa
