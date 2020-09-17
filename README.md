PanGenSpark
=================

Cite as:
Maarala A.I., Arasalo O., Valenzuela D., Heljanko K., Mäkinen V. (2020) Scalable Reference Genome Assembly from Compressed Pan-Genome Index with Spark. In: Nepal S., Cao W., Nasridinov A., Bhuiyan M.Z.A., Guo X., Zhang LJ. (eds) Big Data – BigData 2020. BIGDATA 2020. Lecture Notes in Computer Science, vol 12402. Springer, Cham. https://doi.org/10.1007/978-3-030-59612-5_6

General
-------

This tool can be used to assemble reference genome from a pan-genome, align reads to a pan-genome, and eventually call variants against a pan genome. The pipeline can be used with any species. This tool is an improvement
to PanVC https://gitlab.com/dvalenzu/PanVC. Distributed computing is used as extensively as possible to speed up the computation and make it scale to whole-genome datasets.


Requirements
------------

* python2.7 or newer
* spark 2.0 or newer
* yarn
* HDFS


### Cluster setup
Automated installation with Hadoop distributions such as Hortonworks HDP or Cloudera CDH.

or
 
Manually with minimal configuration by following the instructions in tools/spark-hadoop-init.sh and running the script.


Installation
-------

Compile and Install needed packages by running:
```bash
./compile.sh
```
Modify the ssh credentials, node names and numbering at the end of the script corresponding to your system.


Running the pipeline
---
### Preparing data
---
Download standard human genome version GRCh37 chromosomes 1-22 from https://ftp.ncbi.nlm.nih.gov/genomes/archive/old_genbank/Eukaryotes/vertebrates_mammals/Homo_sapiens/GRCh37/Primary_Assembly/assembled_chromosomes/FASTA/ and unzip to /data/grch37 folder.
Download phased SNP genotype data (includes both haplotypes) from ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ for chromosomes 1-22 to /data/vcfs. Unzip and rename to chr1..22.vcf. 
The whole data set includes 2506 samples generating 13 TB pangenome! You can split the VCF files to smaller test sets eg. 50 samples with command 'cut -f1-59 chr1.vcf'.

or

Use already assembled genomes and skip the vcf2multialign stage (uncomment lines in launch.sh). Copy the assembled genomes in /data/pangenome/ (one line per genome without fasta header).

---
### Modify variables
---

To run the whole pipeline bash scirpt `launch.sh` is used.
The initial configuration assumes at least 22 worker nodes for running chromosomes 1-22 in some stages in parallel (if less used only the numbered chromosomes are assembled).
Modify the static paths to fit your configuration in the launch.sh 

LOCALINDEXPATH=/mnt/tmp # Should be same as was created in the compile.sh

STDREFPATH=/data/grch37 #FASTA files must be divided by chromosomes and named chrN.fa N=1..22

VCFPATH=/data/vcfs #VCF files must be divided by chromosomes and named chrN.vcf N=1..22

HDFSURI=hdfs://namenode:8020 # HDFS URI

NODE=node- #Basename for nodes in the cluster. The nodes must be numbered starting from 1.

---
### Launching
---

```bash
./launch.sh /data/pangenome/ /data/ngsreads/reads.fq.1.fastq /data/ngsreads/reads.fq.2.fastq
```
Here the first parameter is a local filesystem folder where the pangenome (MSA) files are generated to. 
The pangenome data is automatically uploaded to the HDFS with the same folder name.
The next two parameters are are local paired-end read files that are are also uploaded to HDFS and aligned eventually against the pan genome index.

The assembled reference genome is written to HDFS under 'adhoc' folder for each chromosome and eventually downloaded to the local filesystem with the same name.

To be noted
------------------

* At least 2x size of pan-genome in master node is required for local storage and worker nodes should have storage at least 2x of chromosomal pan-genome data size.
* Everything is tested using CentOS 7 with openjdk 1.8
* Each MSA file should only contain a single line (no fasta) and each MSA file has to be of same length.
* If variants should be normalized to a reference genome, the first MSA file alphabetically should be the reference genome e.g GRCh37 chromosome 1 file could be renamed to HG00000.01 (remember to remove fasta header line)

Design
------

1. Data preparation
  * Gapless MSA files are loaded locally per chromosome from HDFS to separate nodes so that hybrid index can
be constructed in parallel per chromosome with CHIC.

2. Hybrid index generation
  * relative lempel ziv parse of the pan genome is calculated in distributed fasion using Apache Spark.
  * chic indexer, a hybrid index developed by Daniel Valenzuela, is called to construct the index used for read alignment.

3. Distributed read alignment
  * index is already distributed to cluster nodes
  * bowtie2 is run independently for different subsets of the read data in the cluster
  * sam files generated by multiple calls to bowtie2 are combined in the master node
  * chic align is called that aligns reads to chromosomal kernels

4. Sam files to pos files
  * Alignment files (.sam) are transformed into position files that only contain the position and length of the match

5. Heaviest path and adhoc reference construction
  * Heaviest path is calculated using Spark
  * Adhoc reference is calculated using Spark

6. Variant calling
  * Samtools and GATK variant calling pipelines are supported to find the variants

