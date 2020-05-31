#!/usr/bin/env bash

#set -e
#set -o pipefail
set -v
set -x

export SPARK_MAJOR_VERSION=2

PGPATHLOCAL=$1
PGPATHHDFS=pg

READS_1=$2
READS_2=$3

LOCALINDEXPATH=/mnt/tmp
STDREFPATH=/data/grch37 #FASTA files must be divided by chromosomes and named chrN.fa N=1..22
VCFPATH=/data/vcfs #VCF files must be divided by chromosomes and named chrN.vcf N=1..22
HDFSURI=hdfs://namenode:8020 # HDFS URI
NODE=node- #Basename for nodes in the cluster. The nodes must be numbered starting from 1.

hdfs dfs -mkdir -p $PGPATHHDFS
date >> runtime.log
echo "Started preparing pan-genome from VCF files with vcf2msa.."
start=`date +%s`
./vcf2msa.sh $PGPATHHDFS $PGPATHLOCAL $STDREFPATH $VCFPATH
end=`date +%s`
runtime=$((end-start))
echo "vcf2msa: ${runtime}" >> runtime.log

date >> runtime.log
echo "Loading files to HDFS..."
hdfs dfs -mkdir -p reads_1&
hdfs dfs -mkdir -p reads_2&
hdfs dfs -put $READS_1 reads_1&
hdfs dfs -put $READS_2 reads_2&

date >> runtime.log
echo "Starting DRLZ.." 
start=`date +%s`
seq 1 22 | xargs -I{} -n 1 -P 4 ./drlz.sh {} $PGPATHHDFS $HDFSURI
runtime=$((end-start))
echo "DRLZ: ${runtime}" >> runtime.log

date >> runtime.log
echo "Starting INDEXING.."
start=`date +%s`
seq 1 22 | xargs -I{} -n 1 -P 22 ssh -tt -o "StrictHostKeyChecking no" $NODE{} /opt/chic/index/index_chr.sh {} $PGPATHHDFS
end=`date +%s`
runtime=$((end-start))
echo "INDEXING: ${runtime}" >> runtime.log

date >> runtime.log
echo "Starting SAMS.."
start=`date +%s`

mkdir -p $LOCALINDEXPATH/sams/
seq 1 22 | xargs -I{} -n 1 -P 22 scp -o "StrictHostKeyChecking no" $NODE{}:$LOCALINDEXPATH/mapped*.sam $LOCALINDEXPATH/sams/

ls $PGPATHLOCAL/* | xargs -I{} -P 22 ./sam2pos.sh {} $LOCALINDEXPATH/sams/ > sam2pos.log

end=`date +%s`
runtime=$((end-start))
echo "SAMS: ${runtime}" >> runtime.log
hdfs dfs -mkdir -p pos

hdfs dfs -put $LOCALINDEXPATH/sams/*.pos pos/

date >> runtime.log
echo "Starting adhoc construction.."

mkdir adhoc
start=`date +%s`
seq 1 22 | xargs -I{} -n 1 -P 2 ./adhoc.sh {} $PGPATHHDFS $PGPATHLOCAL adhoc/chr{} $HDFSURI

end=`date +%s`
runtime=$((end-start))
echo "adhoc: ${runtime}" >> runtime.log
                                                 
