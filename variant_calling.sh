#!/usr/bin/env bash
set -e 
set -o pipefail

export SPARK_MAJOR_VERSION=2
export PATH=$PATH:/opt/bowtie2
export PERL5LIB=/media/pangen/PanVC_distributed/ext_var_call_pipelines/ext/vcftools_0.1.12b/perl

LOG_FILE="runtime.log"

start=`date +%s`

HDFS_PREFIX=hdfs://node-1.novalocal/user/root
# path to plain genome files in HDFS
#HDFS_PLAIN=${HDFS_PREFIX}/plain
# path to MSA files locally

#hdfs path to MSA genome files
HDFS_MSA=${HDFS_PREFIX}/${1}
LOCAL_MSA=$2
READ_1=$3
READ_2=$4


# where to save the index
LOCAL_OUTPUT_FOLDER="output"

TMP="tmp"

N_REFS=$( hdfs dfs -ls ${HDFS_MSA}/* | wc -l )

hdfs dfs -getmerge adhoc adhoc_ref
OUTPUT_FULL="adhoc_ref"
OUTPUT_PLAIN="adhoc_ref.plain"
OUTPUT_FASTA="adhoc_ref.fasta"

cat ${OUTPUT_FULL} | tr -d 'N' > ${OUTPUT_PLAIN}

echo ">adhoc_ref" > ${OUTPUT_FASTA}
cat ${OUTPUT_FULL}  >> ${OUTPUT_FASTA}


ADHOC_PATH="adhoc"
mkdir -p adhoc

mv ${OUTPUT_FASTA} ${ADHOC_PATH}/
mv ${OUTPUT_PLAIN} ${ADHOC_PATH}/
mv ${OUTPUT_FULL} ${ADHOC_PATH}/

#end=`date +%s`
#runtime2=$((end-start_matrix))
#echo "matrix: ${runtime2}" >> $LOG_FILE


############
##  GATK  ##
############

start_ext=`date +%s`

cat ${ADHOC_PATH}/adhoc_ref | tr a-z A-Z > ${ADHOC_PATH}/adhoc_ref_upper
cat ${ADHOC_PATH}/adhoc_ref.plain | tr a-z A-Z > ${ADHOC_PATH}/adhoc_ref_upper.plain
cat ${ADHOC_PATH}/adhoc_ref.fasta | tr a-z A-Z > ${ADHOC_PATH}/adhoc_ref_upper.fasta

EXT_VCF_TOOL_SH=ext_var_call_pipelines/BwaSamtoolsSNPs/pipeline.sh

TMP_VCF_FILE="${ADHOC_PATH}/variants_relative_to_adhoc.vcf"

ADHOC_REFERENCE_PREFIX="${ADHOC_PATH}/adhoc_ref_upper"
ADHOC_REFERENCE_FILE_FASTA="${ADHOC_REFERENCE_PREFIX}.fasta"
${EXT_VCF_TOOL_SH} ${ADHOC_REFERENCE_FILE_FASTA} ${TMP_VCF_FILE} ${READ_1} ${READ_2}


###############
## normalize ##
###############

NORMALIZE_VCF_SH="components/normalize_vcf/normalize.sh"

PAN_GENOME_REF=$( ls ${LOCAL_MSA}/* | sort -n | head -1 )

${NORMALIZE_VCF_SH} ${TMP_VCF_FILE} ${ADHOC_REFERENCE_PREFIX} ${CUR}/${PAN_GENOME_REF}
TMP_NORMALIZED_VCF=${TMP_VCF_FILE}.normalized.vcf
NORMALIZED_VCF=variations.vcf

cp ${TMP_NORMALIZED_VCF} ${NORMALIZED_VCF}

echo "Pipeline succedded. Variations file: ${NORMALIZED_VCF}"


end_ext=`date +%s`
runtime3=$((end_ext-start_ext))
echo "external: ${runtime3}" >> $LOG_FILE
###########
## tests ##
###########


runtime=$((end-start))
echo "total: ${runtime}" >> $LOG_FILE

#metrics
#python tools/metrics.py -r NA12872-1 -t variations.vcf -v variations_standard.vcf -o ${LOG_FILE}
#python tools/p_distance.py -r NA20811-1 -a adhoc_ref/adhoc_ref >> ${LOG_FILE}

#compression
cat ${LOCAL_OUTPUT_FOLDER}/index.log | grep 'Original length n' >> ${LOG_FILE}
cat ${LOCAL_OUTPUT_FOLDER}/index.log | grep 'Kernel text length n' >> ${LOG_FILE}
