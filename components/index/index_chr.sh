#!/usr/bin/env bash
set -e 
set -o pipefail
set -v
set -x

CHR=$1
HDFSPGPATH=$2
HDFSLZPATH=$2drlz
LOCALPATH=/mnt/tmp
cpus=$(lscpu -p | tail -n+5 | wc -l)

echo Executing index_chr.sh $1 $2 with $cpus cpus 

    i=$(printf "%02d" $CHR)
    hdfs dfs -getmerge $HDFSLZPATH/*.$i.lz $LOCALPATH/chr$i.lz&
    hdfs dfs -get -f reads_1/* $LOCALPATH/reads_1.fq&
    hdfs dfs -get -f reads_2/* $LOCALPATH/reads_2.fq&
    rm -f $LOCALPATH/chr$i.fa
    hdfs dfs -getmerge $2gapless/*$i* $LOCALPATH/chr$i.fa

    /opt/chic/index/chic_index --threads=$cpus  --kernel=BOWTIE2 --verbose=1 --lz-input-plain-file=$LOCALPATH/chr$i.lz $LOCALPATH/chr$i.fa 80 2> chic_index.log
    /opt/chic/index/chic_align -v1 -t $cpus -o $LOCALPATH/aligned.sam /mnt/tmp/chr$i.fa $LOCALPATH/reads_1.fq $LOCALPATH/reads_2.fq 2> chic_align.log
    /opt/samtools/samtools view -F4 $LOCALPATH/aligned.sam > $LOCALPATH/mapped$i.sam
