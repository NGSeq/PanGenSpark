#!/usr/bin/env bash

#Simple bwa+samtools+bcftools variant calling pipeline
#Requires bwa,samtools and bcftools to be installed

REFERENCE_FILE=${1}

OUT=${2}

READS_FILE_1=${3}

READS_FILE_2=${4}

date >> exectime
bwa index -a bwtsw $REFERENCE_FILE
date >> exectime
bwa mem -t 48 $REFERENCE_FILE $READS_FILE_1 $READS_FILE_2 > $OUT/aligned.sam
date >> exectime
samtools view -Sb $OUT/aligned.sam > $OUT/aligned.bam
samtools sort $OUT/aligned.bam -o $OUT/sorted-alns2.bam
date >> exectime
samtools mpileup -uf $REFERENCE_FILE $OUT/sorted-alns2.bam > $OUT/pileup.vcf
bcftools view $OUT/pileup.vcf > $OUT/var.raw.vcf
bcftools view $OUT/var.raw.vcf | vcfutils.pl varFilter -D100 > $OUT/var.flt.vcf
date >> exectime

