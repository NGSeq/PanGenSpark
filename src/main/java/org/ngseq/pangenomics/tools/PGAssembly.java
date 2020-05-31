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

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.FastqInputFormat.FastqRecordReader;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**Usage
 *  spark-submit --master local[4] --class fi.aalto.ngs.seqspark.pangenomics.ParallelCHICAligner target/pangenomics-0.9-jar-with-dependencies.jar -fastq reads.fq -split 1 -splitout /user/root/fqsplits -ref /media/data/dev/PanVC/example_data/reference_pg_aligned.index/recombinant.all.fa -bwaout /user/root/bwaout -numsplits 1 -align

 Pan-Genomic assemblies (or even individuals) are not typically provided publicly, thus we have to assembly the Pan-genome from variants called from multiple individuals.
 We can reuse the pipeline also for calling variants in parallel per individual.
 Variants of multiple individuals  can be provided also in one vcf file.

 See http://www.internationalgenome.org/category/assembly/

 Split vcf by individuals and assembly in parallel.

 **/


public class PGAssembly {
  private static final String VCFTOOLS = "vcftools";

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  private static String alignmentTable = "alignedreads";
  private static String rawreadsTable = "rawreads";


  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("ParallelBWAMem");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc.sc());

    //String query = args[2];

    Options options = new Options();
    Option pathOpt = new Option( "fastq", true, "Path to fastq file in hdfs." );
    Option path2Opt = new Option( "fastq2", true, "Path to fastq file in hdfs." );
    Option splitDirOpt = new Option( "splitout", true, "Path to output directory in hdfs." );
    Option prefixOpt = new Option( "prefix", true, "Prefix of input haplotype files. Usually string before .phased extension without chromosome number." );
    Option splitsOpt = new Option( "numsplits", true, "Number of read splits, depends on the size of read file, number of cores and available memory." );
    //gmOpt.setRequired(true);
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );
    Option opOpt = new Option( "bwaout", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option pairedOpt = new Option( "paired", "Use paired end reads" );

    Option queryOpt = new Option( "query", true, "SQL query string." );

    Option splitOpt = new Option( "split", "Perform read file splitting" );
    Option alignOpt = new Option( "align", "Perform BWA alignment" );
    Option samOpt = new Option( "samopts", true, "Options for samtools view given within parenthesis e.g.'-F 4'" );
    Option filterOpt = new Option( "filtersam", "" );

    options.addOption( pathOpt );
    options.addOption( path2Opt );
    options.addOption( prefixOpt );
    options.addOption( splitsOpt );
    options.addOption( refOpt );
    options.addOption( opOpt );
    options.addOption( splitDirOpt );
    options.addOption( pairedOpt );
    options.addOption( queryOpt );
    options.addOption( splitOpt );
    options.addOption( alignOpt );
    options.addOption( samOpt );
    options.addOption( filterOpt );
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
    String splitDir = (cmd.hasOption("splitout")==true)? cmd.getOptionValue("splitout"):null;
    String READS_FILE = cmd.getOptionValue("fastq");
    //String fastq2 = (cmd.hasOption("fastq2")==true)? cmd.getOptionValue("fastq2"):null;
    String SEQUENCE_ALL_FILE = (cmd.hasOption("ref")==true)? cmd.getOptionValue("ref"):null;
    int splits = (cmd.hasOption("numsplits")==true)? Integer.parseInt(cmd.getOptionValue("numsplits")):0;
    String inputPrefix = cmd.getOptionValue("prefix");
    String bwaOutDir = (cmd.hasOption("bwaout")==true)? cmd.getOptionValue("bwaout"):null;
    boolean paired = cmd.hasOption("paired");
    String samtoolopts = (cmd.hasOption("samopts")==true)? cmd.getOptionValue("samopts"):"";
    boolean filtersam = cmd.hasOption("filtersam");
    String query = (cmd.hasOption("query")==true)? cmd.getOptionValue("query"):null;


    final String finalSplitDir = splitDir;
    final String finalBwaOutDir = bwaOutDir;

    String splitDir2 = null;
    if(paired)
      splitDir2 = splitDir+"2";
    final String finalSplitDir2 = splitDir2;

    //TODO: handle compressed files, use LZO if possible as it is splittable format
    FileSystem fs = FileSystem.get(new Configuration());
    if(cmd.hasOption("fastq")){
      long flen;
      if(READS_FILE.startsWith("file://")){
        //TODO: treat local files differently, move to hdfs or split by external commands
        File f = new File(READS_FILE);
        flen = f.length();
      }else{
        FileStatus fstatus = fs.getFileStatus(new Path(READS_FILE));
        flen = fstatus.getLen();
      }
      //Count split positions

      //Split fastq files if split option is given
      if(cmd.hasOption("split")){

        long splitlen = flen/splits;
        ArrayList<Integer> numList = new ArrayList<>();
        for(int i = 0; i<splits; i++)
          numList.add(i);

        JavaRDD<Integer> splitRDD = sc.parallelize(numList);
        splitFastq(splitRDD, READS_FILE, splitDir, splitlen);
        /*if(paired){
          splitFastq(splitRDD, fastq2, splitDir2, splitlen);
        }*/
      }
    }

    //This could be used when reference has to be loaded in memory
    //sc.addFile(ref);

    FileStatus[] st = fs.listStatus(new Path(splitDir));
    ArrayList<String> splitFileList = new ArrayList<>();
    for (int i=0;i<st.length;i++){
      splitFileList.add(st[i].getPath().getName().toString());
    }
    JavaRDD<String> splitFilesRDD = sc.parallelize(splitFileList);

    //Do bwa alignment if align option is given

      JavaRDD<String> outRDD = splitFilesRDD.map(fname -> {

          System.out.println(fname);
          Process process;
             //String command = "bwa mem " + ref + " <(hdfs dfs -text "+splitDir+"/"+fname+") | hdfs dfs -put /dev/stdin " + bwaOutDir + "/" + fname + ".sam";

            //ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", "hdfs dfs -text "+ finalSplitDir +"/"+fname+" | bwa mem -M " + ref + " /dev/stdin | samtools view "+samtoolopts+" -h /dev/stdin | hdfs dfs -put /dev/stdin " + finalBwaOutDir + "/" + fname + ".sam");

            String command =  "hdfs dfs -text "+ finalSplitDir +"/"+fname+" | "+VCFTOOLS+ " "+SEQUENCE_ALL_FILE+" /dev/stdin | hdfs dfs -put /dev/stdin " + finalBwaOutDir + "/" + fname + ".sam ";
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

    System.out.println(outRDD.count());

 /*   SparkLoadSAM srb = new SparkLoadSAM(sc, bwaOutDir, false);

    //Map to SAMRecord RDD
    JavaRDD<SAMRecord> bamRDD = srb.getRecords().map(v1 -> v1._2().get());
*/
    new HDFSWriter(outRDD, bwaOutDir, "sam", sc);

    sc.stop();

  }

  private static JavaPairRDD<String, MyAlignment> mapSAMRecordsToMyAlignments(JavaRDD<SAMRecord> bamRDD) {

    //Map SAMRecords to MyReads
    JavaPairRDD<String, MyAlignment> myalignmentsRDD = bamRDD.mapToPair(read -> {

      MyAlignment myalignment = new MyAlignment();
      //myread.setReadgroup(read.getReadGroup());
      myalignment.setReadName(read.getReadName());
      myalignment.setReferenceName(read.getReferenceName());
      myalignment.setStart(read.getStart());
      myalignment.setBases(new String(read.getReadBases(), StandardCharsets.UTF_8));
      myalignment.setCigar(read.getCigar().toString());
      myalignment.setReadUnmapped(read.getReadUnmappedFlag());
      myalignment.setLength(read.getReadLength());

      return new Tuple2<String, MyAlignment>(myalignment.getReadName(), myalignment);
    });
    return myalignmentsRDD;
  }


  //TODO: Implement pileup dataframe from Heaviest path and do query on that

  private static void splitFastq(JavaRDD<Integer> splitRDD, String fqPath, String splitDir, long splitlen) throws IOException {
    Path fqpath = new Path(fqPath);
    String fqname = fqpath.getName();
    String[] ns = fqname.split("\\.");
    //TODO: Handle also compressed files
    splitRDD.foreach(num -> {
      FileSplit split = new FileSplit(new Path(fqPath), splitlen * num, splitlen, null);
      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), split);
      writeFastqSplits(fqreader, new Configuration(), splitDir + "/split_" + num + "." + ns[1]);
    });

  }

 private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long length) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, length, null);

    return new FastqRecordReader(config, split);
  }

  private static void writeFastqSplits(FastqRecordReader fqreader, Configuration config, String fileName){

    ByteArrayOutputStream os = new ByteArrayOutputStream();

    FSDataOutputStream dataOutput = null;
    try {
      FileSystem fs = FileSystem.get(config);
      dataOutput = new FSDataOutputStream(os);
      dataOutput = fs.create(new Path(fileName));
    } catch (IOException e) {
      e.printStackTrace();
    }

    FastqOutputFormat.FastqRecordWriter writer = new FastqOutputFormat.FastqRecordWriter(config, dataOutput);

    try {
      boolean next = true;
      while(next) {
        Text t = fqreader.getCurrentKey();
        SequencedFragment sf = fqreader.getCurrentValue();
        next = fqreader.next(t, sf);
        writer.write(fqreader.getCurrentKey(), fqreader.getCurrentValue());
      }

      dataOutput.close();
      os.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}