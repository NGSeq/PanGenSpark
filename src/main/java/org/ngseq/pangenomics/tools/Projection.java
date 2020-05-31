package org.ngseq.pangenomics.tools;/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**Usage
 * Reference fasta must be already partitioned to HDFS
 *  spark-submit --master local[4] --class fi.aalto.ngs.seqspark.pangenomics.ReferenceNormalization target/pangenomics-0.9-jar-with-dependencies.jar -reference hg38 -vcf variations.vcf -vcfout vcfout

 TMP_VCF_FILE="${ADHOC_REF_OUTPUT_FOLDER}/variants_relative_to_adhoc.vcf"

 ADHOC_REFERENCE_PREFIX="${ADHOC_REF_OUTPUT_FOLDER}/adhoc_reference"
 ADHOC_REFERENCE_FILE_FASTA="${ADHOC_REFERENCE_PREFIX}.fasta"

 1.   ############### PREPROCESSING FOR NORMALIZATION #########################################
     ${EXT_VCF_TOOL_SH} ${ADHOC_REFERENCE_FILE_FASTA} ${READS_FILE} ${TMP_VCF_FILE}

     PARALLELIZE BY READS:

                ####### EXT_VCF_TOOL_SH == pipeline.sh AND IT DOES FOLLOWING ######
     Index ADHOC FASTA with BWA INDEX if not done -> BWA MEM for original reads against ADHOC ref -> sort BAM/SAMs
      -> Do PILEUP for sorted | bcftools view -bvcg  for pileup | bcftools view var.raw.bcf | vcfutils.pl varFilter -D100 > var.flt.vcf

     INDEX_FILE=${REFERENCE_FILE}.bwt
     if [ ! -f "${INDEX_FILE}" ]; then
         echo "Index not found, indexing reference... '${INDEX_FILE}'not found!"
         ${BWA_BIN} index $REFERENCE_FILE
         else
         echo "Index file  '${INDEX_FILE}' found, reusing it."
     fi

     ${BWA_BIN} mem $REFERENCE_FILE $READS_FILE > aligned.sam
     ${SAMTOOLS_BIN} view -Sb aligned.sam > aligned.bam
     ${SAMTOOLS_BIN} sort -f aligned.bam  sorted-alns2.bam
     ${SAMTOOLS_BIN} mpileup -uf $REFERENCE_FILE  sorted-alns2.bam | ${BCFTOOLS_BIN} view -bvcg - > 0var.raw.bcf
     ${BCFTOOLS_BIN} view var.raw.bcf | ${VCFUTILS_BIN} varFilter -D100 > var.flt.vcf

     mv var.flt.vcf $OUTPUT_FILE
     rm var.*cf
     rm *am

     ######################


     # Now we "normalize" the vcf, that is, we map it to the original reference instead to the adhoc
     # reference.
     ${NORMALIZE_VCF_SH} ${TMP_VCF_FILE} ${ADHOC_REFERENCE_PREFIX} ${PAN_GENOME_REF}

 2.    ################# NORMALIZATION DOES FOLLOWING #############

       ## APLY VCF TO TMP VARS TO ADHOC REF
       ALIGNED_VARS="${TMP_VCF_FILE}.applied"
      #####applies variants to adhoc reference
       echo "[normalize.sh] will call command: python ${DIR}/apply_vcf.py $ADHOC_REFERENCE_FILE_FASTA $TMP_VCF_FILE > ${ALIGNED_VARS}"
            python ${DIR}/apply_vcf.py $ADHOC_REFERENCE_FILE_FASTA $TMP_VCF_FILE > ${ALIGNED_VARS}

      ####set Original reference and adhoc ref files##
       A1=${PAN_REFERENCE_FOLDER}/recombinant.n1.full
       A2=${ADHOC_REFERENCE_ALIGNED_TO_REF}
      ##set temp files for writing
       X1="${TMP_VCF_FILE}.applied.seq1"
       X2="${TMP_VCF_FILE}.applied.seq2"
      ##Take just reference sequences from  adhoc with variants and add to temp files without line endings##
       head -n1 ${ALIGNED_VARS} | tr -d '\n' > ${X1}
       tail -n1 ${ALIGNED_VARS} | tr -d '\n' > ${X2}
      ##check that original adhoc and variants applied to original reference equal?
       ${DIR}/validate_equal_sequences.sh ${A2} ${X1}

       OUTPUT_PREFIX="${TMP_VCF_FILE}"
 3.  ### PROJECT orig and and adhoc refs + variants applied to orig and adhoc refs
       echo "${DIR}/projector/projector ${A1} ${A2} ${X1} ${X2} ${OUTPUT_PREFIX}"
       ${DIR}/projector/projector ${A1} ${A2} ${X1} ${X2} ${OUTPUT_PREFIX}
4.  ### DO MSA for projected sequence, outputs normalized variants
       MSA=${OUTPUT_PREFIX}.msa
       MSA_TO_VCF=${DIR}/jvarkit/dist-1.128/biostar94573
       NORMALIZED_VCF=${TMP_VCF_FILE}.normalized.vcf

       ${MSA_TO_VCF} ${MSA} > ${NORMALIZED_VCF}

 ##########################


 TMP_NORMALIZED_VCF=${TMP_VCF_FILE}.normalized.vcf
 NORMALIZED_VCF=${WORKING_FOLDER}/variations.vcf

 cp ${TMP_NORMALIZED_VCF} ${NORMALIZED_VCF}

 echo "Pipeline succedded. Variations file: ${NORMALIZED_VCF}"

 **/


public class Projection {

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("ReferenceNormalization");
    JavaSparkContext sc = new JavaSparkContext(conf);

    Options options = new Options();
    Option path2Opt = new Option( "vcf", true, "Path to vcf file in hdfs." );
    Option refDirOpt = new Option( "reference", true, "Path to reference files directory in HDFS. Files can splitted by chromosomal level or partitioned by HDFS" );
    //gmOpt.setRequired(true);
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );
    Option opOpt = new Option( "vcfout", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );

    options.addOption( path2Opt );
    options.addOption( refOpt );
    options.addOption( opOpt );
    options.addOption( refDirOpt );
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      // parse the command line arguments
      cmd = parser.parse( options, args );

    }
    catch( ParseException exp ) {
      // oops, something went wrong
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
    }
    String refDir = (cmd.hasOption("reference")==true)? cmd.getOptionValue("reference"):null;
    String FASTA_FILE = cmd.getOptionValue("fasta");
    String VCF_FILE = (cmd.hasOption("vcf")==true)? cmd.getOptionValue("vcf"):null;
    String vcfOutDir = (cmd.hasOption("vcfout")==true)? cmd.getOptionValue("vcfout"):null;

    final String finalRefDir = refDir;
    final String finalvcfoutDir = vcfOutDir;

    FileSystem fs = FileSystem.get(new Configuration());
    
    FileStatus[] st = fs.listStatus(new Path(refDir));
    ArrayList<String> splitFileList = new ArrayList<>();
    for (int i=0;i<st.length;i++){
      splitFileList.add(st[i].getPath().getName().toString());
    }

    JavaRDD<String> appliedRDD = sc.parallelize(splitFileList);

    JavaRDD<String> projectedRDD = appliedRDD.map(fname -> {

      System.out.println(fname);
      Process process;

      //String command = "bwa mem " + ref + " <(hdfs dfs -text "+refDir+"/"+fname+") | hdfs dfs -put /dev/stdin " + vcfoutDir + "/" + fname + ".sam";

      //ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", "hdfs dfs -text "+ finalRefDir +"/"+fname+" | bwa mem -M " + ref + " /dev/stdin | samtools view "+samtoolopts+" -h /dev/stdin | hdfs dfs -put /dev/stdin " + finalvcfoutDir + "/" + fname + ".sam");

      //hadoop pipes -D hadoop.pipes.java.recordreader=true  -D hadoop.pipes.java.recordwriter=true -input dft1  -output dft1-out -program
      //       /media/data/dev/PanVC/components/normalize_vcf/projector/projector ${A1} ${A2} ${X1} ${X2} ${OUTPUT_PREFIX}
      //TODO: modify projector to just print to stdout
      //TODO: Files should be splitted to 4 different directories with equal size partitions or parallelization could be done in chromosome level perhaps more easily as references originally already splitted
      String command =  "/media/data/dev/PanVC/components/normalize_vcf/projector/projector ${A1} ${A2} ${X1} ${X2} ${OUTPUT_PREFIX} <(hdfs dfs -text "+ finalRefDir +"/"+fname+") <(hdfs dfs -text "+ finalRefDir +"/"+fname+") | hdfs dfs -put /dev/stdin " + finalvcfoutDir + "/" + fname + ".fa ";
      System.out.println(command);

      ProcessBuilder pbb = new ProcessBuilder("/bin/sh", "-c", command);
      //pb.environment().forEach((k,v)->System.out.println("Var : " + k + " Val : " + v));
      pbb.command().forEach((k)->System.out.println("cmd : " + k));
      //ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

      process = pbb.start();

      InputStream is = process.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);

      String out ="";
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
        out += line;
      }
      return out;
    });
    System.out.println(projectedRDD.count());
    sc.stop();

  }

}