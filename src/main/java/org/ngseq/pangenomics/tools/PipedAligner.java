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

import htsjdk.samtools.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.seqdoop.hadoop_bam.FastqInputFormat.FastqRecordReader;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import scala.Tuple2;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**Usage
 * It's all about right configuration and memory allocation
 *  spark-submit --master local[4] --class fi.aalto.ngs.seqspark.metagenomics.PipedAligner target/metagenomics-0.9-jar-with-dependencies.jar -fastqdir /user/root/fqsplits -ref /media/data/genomes/hg38/hg38.fa -bwaout /user/root/bwaout -align
 spark-submit --master yarn-cluster --driver-cores 5 --num-executors 10 --executor-memory 3g --conf spark.yarn.executor.memoryOverhead=3000 --conf spark.task.maxFailures=40 --conf spark.yarn.max.executor.failures=1000 --conf spark.hadoop.validateOutputSpecs=true --class fi.aalto.ngs.seqspark.metagenomics.Piped target/metagenomics-0.9-jar-with-dependencies.jar -fastqdir /user/root/fqsplits -ref /index/hg38.fa -bwaout /user/hdfs/bwaout -align

 **/
//FIXME: On Yarn, Lost executor failures, perhaps memory issues or timeout
//http://stackoverflow.com/questions/32038360/spark-executor-lost-because-of-time-out-even-after-setting-quite-long-time-out-v
//http://pastebin.com/B4FbXvHR

public class PipedAligner {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  private static String alignmentTable = "alignedreads";
  private static String rawreadsTable = "rawreads";

  public static <R> void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("PipedAligner");
    JavaSparkContext sc = new JavaSparkContext(conf);

    //String query = args[2];

    Options options = new Options();
    Option splitDirOpt = new Option( "fastqdir", true, "Path to output directory in hdfs." );
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );
    Option opOpt = new Option( "bwaout", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option pairedOpt = new Option( "paired", "Use paired end reads" );
    Option queryOpt = new Option( "query", true, "SQL query string." );
    Option alignOpt = new Option( "align", "Perform BWA alignment" );
    Option samOpt = new Option( "samopts", true, "Options for samtools view given within parenthesis e.g.'-F 4'" );
    Option filterOpt = new Option( "filtersam", "" );
    Option writeOpt = new Option( "writedirect", "write directly to HDFS without using partitioning with Hadoop-BAM" );

    options.addOption( refOpt );
    options.addOption( opOpt );
    options.addOption( splitDirOpt );
    options.addOption( pairedOpt );
    options.addOption( queryOpt );
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
    String splitDir = (cmd.hasOption("fastqdir")==true)? cmd.getOptionValue("fastqdir"):null;
    String ref = (cmd.hasOption("ref")==true)? cmd.getOptionValue("ref"):null;
    String bwaOutDir = (cmd.hasOption("bwaout")==true)? cmd.getOptionValue("bwaout"):null;
    boolean paired = cmd.hasOption("paired");
    String samtoolopts = (cmd.hasOption("samopts")==true)? cmd.getOptionValue("samopts"):"";
    boolean filtersam = cmd.hasOption("unmapped");
    boolean writedirect = cmd.hasOption("writedirect");

    String query = (cmd.hasOption("query")==true)? cmd.getOptionValue("query"):null;


    final String finalSplitDir = splitDir;
    final String finalBwaOutDir = bwaOutDir;

    String splitDir2 = null;
    if(paired)
      splitDir2 = splitDir+"2";
    final String finalSplitDir2 = splitDir2;

    //TODO: handle compressed files, use LZO if possible as it is splittable format
   /* FileStatus[] st2 = fs.listStatus(new Path(splitDir2));
    ArrayList<Tuple2<String,String>> splitFileList = new ArrayList<>();
    for (int i=0;i<st.length;i++){
      splitFileList.add(new Tuple2<String, String>(st[i].getPath().getName().toString(), st2[i].getPath().getName().toString()));
    }
    JavaRDD<Tuple2<String, String>> splitFilesRDD = sc.parallelize(splitFileList);
    */

    FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
    FileStatus[] st = fs.listStatus(new Path(splitDir));
    ArrayList<String> splitFileList = new ArrayList<>();
    for (int i=0;i<st.length;i++){
      splitFileList.add(st[i].getPath().getName().toString());
    }
    JavaRDD<String> splitFilesRDD = sc.parallelize(splitFileList);

    //conf.set("spark.default.parallelism", String.valueOf(splitFileList.size()/2));
    long c = splitFilesRDD.count();
    //Do bwa alignment if align option is given
    if(cmd.hasOption("align")){
      JavaRDD<String> alignmentRDD = splitFilesRDD.map(fname -> {
        System.out.println(fname);
        Process process;
        if (paired) {
          //| samtools view "+samtoolopts+"
          //bwa mem /tmp/adhoc.fa <(hdfs dfs -text /user/root/fqsplits/split_1.fq) <(hdfs dfs -text /user/root/fqsplits2/split_1.fq) | hdfs dfs -put /dev/stdin " + finalBwaOutDir + "/" + fname + ".sam "
          String command = "bwa mem " + ref + " <(hdfs dfs -text " + finalSplitDir + "/" + fname + ") <(hdfs dfs -text " + finalSplitDir2 + "/" + fname + ")";
          System.out.println(command);

          ProcessBuilder pbb = new ProcessBuilder("/bin/sh", "-c", command);
          //pb.environment().forEach((k,v)->System.out.println("Var : " + k + " Val : " + v));
          pbb.command().forEach((k) -> System.out.println("cmd : " + k));
          pbb.redirectError(ProcessBuilder.Redirect.INHERIT);
          //System.out.println("dir : "+ pb.directory().toString());
          process = pbb.start();

        } else {
          //String command = "bwa mem " + ref + " <(hdfs dfs -text "+splitDir+"/"+fname+") | hdfs dfs -put /dev/stdin " + bwaOutDir + "/" + fname + ".sam";  | samtools view -f 4 | hdfs dfs -put /dev/stdin " + finalBwaOutDir + "/" + fname + ".sam"
          String command = "hdfs dfs -text " + finalSplitDir + "/" + fname + " | bwa mem -p " + ref + " /dev/stdin";

          if(writedirect)
            command = "hdfs dfs -text " + finalSplitDir + "/" + fname + " | bwa mem -p " + ref + " /dev/stdin "+ ((filtersam == true) ? " | samtools view -f 4 ":"") + "| hdfs dfs -put /dev/stdin " + finalBwaOutDir + "/" + fname + ".sam";
          ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);
          //ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

          pb.command().forEach((k) -> System.out.println("cmd : " + k));
          pb.redirectError(ProcessBuilder.Redirect.INHERIT);
          process = pb.start();
        }

        InputStream is = process.getErrorStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String out = "";
        String line;
        while ((line = br.readLine()) != null) {
          out += line + System.lineSeparator();
        }
        process.waitFor();
        is.close();
        return out;

      });

      //rdd.collect();
      if(!writedirect){
        JavaRDD<SAMRecord>  samRDD = alignmentRDD.map(aln -> {
          //TODO: build SAMRecord right
          SAMFileHeader header = new SAMFileHeader();
          //header.setTextHeader(aln.split("\\t")[0].split(":")[0]);
          header.setTextHeader("@HD\\tVN:0.7.15-r1140\\tSO:unknown\\n@SQ\\tSN:"+ref+"");
          //TODO:If validation used, parsing fails,check alignment fields, reference name is wrong
          //FIXME: htsjdk.samtools.SAMFormatException: Error parsing text SAM file. length(QUAL) != length(SEQ); Line unknown
          aln = aln.replace("\r\n", "").replace("\n", "").replace(System.lineSeparator(), "");
          SAMRecord record = new SAMRecord(header);
          try{
            final SAMLineParser samLP = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.DEFAULT_STRINGENCY, header, null, null);
            record = samLP.parseLine(aln);

          }catch(SAMFormatException e){
            System.out.println(e.getMessage().toString());
          }
          return record;
        });

        if(filtersam)
          samRDD.filter(v1 -> v1.getReadUnmappedFlag());

        //Map and save
        SAMFileHeader header = new SAMFileHeader();
        //header.setTextHeader(alignments.take(1).get(0).split("\\t")[0].split(":")[0]);
        header.setTextHeader("@HD\\tVN:0.7.15-r1140\\tSO:unknown\\n@SQ\\tSN:"+ref+"");
        final Broadcast<SAMFileHeader> headerBc = sc.broadcast(header);

        //IF sam
        // sc.hadoopConfiguration().setBoolean(KeyIgnoringAnySAMOutputFormat.WRITE_HEADER_PROPERTY, true);
        BAMHeaderOutputFormat.setHeader(headerBc.getValue());
        JavaRDD<SAMRecord> samHRDD = setPartitionHeaders(samRDD, headerBc);
        JavaPairRDD<SAMRecord, SAMRecordWritable> samWritableRDD = readsToWritable(samHRDD, headerBc);

        samWritableRDD.saveAsNewAPIHadoopFile(bwaOutDir, SAMRecord.class, SAMRecordWritable.class, BAMHeaderOutputFormat.class, sc.hadoopConfiguration());
      }
      //rdd.count();
    }
    //TODO: create Hadoop BAM SamRecords from std output or written files
    //TODO: modify SparkReadBAM to read from path, see SparkSQLExmple

    sc.stop();

  }

  private static JavaPairRDD<SAMRecord, SAMRecordWritable> readsToWritable(JavaRDD<SAMRecord> records, Broadcast<SAMFileHeader> header) {
    return records.mapToPair(read -> {
      read.setHeaderStrict(header.getValue());
      final SAMRecordWritable samRecordWritable = new SAMRecordWritable();
      samRecordWritable.set(read);
      return new Tuple2<>(read, samRecordWritable);
    });
  }

  private static JavaRDD<SAMRecord> setPartitionHeaders(final JavaRDD<SAMRecord> reads, final Broadcast<SAMFileHeader> header) {

    return reads.mapPartitions(readIterator -> {
      BAMHeaderOutputFormat.setHeader(header.getValue());
      return readIterator;
    });
  }

  public static class BAMHeaderOutputFormat extends KeyIgnoringBAMOutputFormat<NullWritable> {
    public static SAMFileHeader samheader;
    public static boolean writeHeader = true;

    public static void setHeader(final SAMFileHeader header) {
      samheader = header;
    }

    public void setWriteHeader(final boolean s) {
      writeHeader = s;
    }

    @Override
    public RecordWriter<NullWritable, SAMRecordWritable> getRecordWriter(TaskAttemptContext ctx,
                                                                         Path outputPath) throws IOException {
      // the writers require a header in order to create a codec, even if
      // the header isn't being written out
      setSAMHeader(samheader);
      setWriteHeader(writeHeader);

      return super.getRecordWriter(ctx, outputPath);
    }
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


 private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long length) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, length, null);

    return new FastqRecordReader(config, split);
  }

}