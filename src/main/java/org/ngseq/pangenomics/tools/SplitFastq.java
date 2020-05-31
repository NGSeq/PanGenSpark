package org.ngseq.pangenomics.tools;


import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat.FastqRecordReader;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**Usage
 *  spark-submit --master local[4] --class fi.aalto.ngs.seqspark.metagenomics.SplitFastq target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/fw.fq -fastq2 /user/root/rw.fq -splitsize 10000 -splitout /user/root/fqsplits -interleave
   **/


public class SplitFastq {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  private static int splitlen;
  private static JavaSparkContext sc;


  public static <U> void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SplitFastq");
    sc = new JavaSparkContext(conf);

    //String query = args[2];

    Options options = new Options();
    Option pathOpt = new Option( "fastq", true, "Path to fastq file in hdfs." );
    Option path2Opt = new Option( "fastq2", true, "Path to fastq file in hdfs." );
    Option splitDirOpt = new Option( "splitout", true, "Path to output directory in hdfs." );
    Option numsplitsOpt = new Option( "splitsize", true, "Number of reads in split, depends on the size of read file, number of cores and available memory." );
    Option pairedOpt = new Option( "paired", "Use paired end reads" );
    Option intOpt = new Option( "interleave", "" );

    options.addOption( pathOpt );
    options.addOption( path2Opt );
    options.addOption( numsplitsOpt );
    options.addOption( splitDirOpt );
    options.addOption( pairedOpt );
    options.addOption( intOpt );
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
    String fastq = cmd.getOptionValue("fastq");
    String fastq2 = (cmd.hasOption("fastq2")==true)? cmd.getOptionValue("fastq2"):null;
    int splitsize = (cmd.hasOption("splitsize")==true)? Integer.parseInt(cmd.getOptionValue("splitsize")):0;
    boolean paired = cmd.hasOption("paired");
    boolean interleave = cmd.hasOption("interleave");


    String splitDir2 = null;
    if(paired)
      splitDir2 = splitDir+"2";

    //TODO: handle compressed files, use LZO if possible as it is splittable format
    FileSystem fs = FileSystem.get(new Configuration());

      if(fastq.startsWith("file://")){
        //TODO: treat local files differently, move to hdfs or split by external commands
        File f = new File(fastq);
      }else{

      }
      //Count split positions
      splitlen = splitsize*4; //reads are expressed by 4 lines

      if(interleave){
        FileStatus fst = fs.getFileStatus(new Path(fastq));
        FileStatus fst2 = fs.getFileStatus(new Path(fastq2));
        long len2 = fs.getFileStatus(new Path(fastq2)).getLen();

        interleaveSplitFastq(fst, fst2, splitDir, splitsize );
      }else{
        FileStatus fstatus = fs.getFileStatus(new Path(fastq));
        //int lines = (int)(fstatus.getLen()/splitsize)/(8*550); //approximate one read has 150bp*3lines+100Bytes for name => 550Bytes/read
        splitFastq(fstatus, fastq, splitDir, splitlen);
        if(paired){
          FileStatus fstatus2 = fs.getFileStatus(new Path(fastq2));
          splitFastq(fstatus2, fastq2, splitDir2, splitlen);
        }
      }

    sc.stop();

  }


  private static void splitFastq(FileStatus fst, String fqPath, String splitDir, int splitlen) throws IOException {
    Path fqpath = new Path(fqPath);
    String fqname = fqpath.getName();
    String[] ns = fqname.split("\\.");
    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);

    splitRDD.foreach( split ->  {

      FastqRecordReader fqreader = new FastqRecordReader(sc.hadoopConfiguration(), split);
      writeFastqFile(fqreader, new Configuration(), splitDir + "/split_" + split.getStart() + "." + ns[1]);

     });
  }

  private static void writeInterleavedSplits(FastqRecordReader fqreader, FastqRecordReader fqreader2, Configuration config, String fileName){

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
      boolean next2 = true;
      while(next) {
        Text t = fqreader.getCurrentKey();
        SequencedFragment sf = fqreader.getCurrentValue();
        next = fqreader.next(t, sf);
        writer.write(fqreader.getCurrentKey(), fqreader.getCurrentValue());

        //TODO: write from another reader here

        Text t2 = fqreader2.getCurrentKey();
        SequencedFragment sf2 = fqreader2.getCurrentValue();
        next2 = fqreader2.next(t2, sf2);
        writer.write(fqreader2.getCurrentKey(), fqreader2.getCurrentValue());
      }

      dataOutput.close();
      os.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void interleaveSplitFastq(FileStatus fst, FileStatus fst2, String splitDir, int splitlen) throws IOException {

    String[] ns = fst.getPath().getName().split("\\.");
    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);
    List<FileSplit> nlif2 = NLineInputFormat.getSplitsForFile(fst2, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);
    JavaRDD<FileSplit> splitRDD2 = sc.parallelize(nlif2);
    JavaPairRDD<FileSplit, FileSplit> zips = splitRDD.zip(splitRDD2);

    zips.foreach( splits ->  {

      FastqRecordReader fqreader = new FastqRecordReader(sc.hadoopConfiguration(), splits._1);
      FastqRecordReader fqreader2 = new FastqRecordReader(sc.hadoopConfiguration(), splits._2);
      writeInterleavedSplits(fqreader, fqreader2, sc.hadoopConfiguration(), splitDir+"/"+splits._1.getStart()+ "." + ns[1]);
    });
  }

 private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long length) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, length, null);

    return new FastqRecordReader(config, split);
  }

  private static void writeFastqFile(FastqRecordReader fqreader, Configuration config, String fileName){

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

  //split fastq cmd way
    /*Process lines = new ProcessBuilder("/bin/sh", "-c", "hdfs dfs -cat "+fastq+" | wc -l ").start();
    BufferedReader brl = new BufferedReader(new InputStreamReader(lines.getInputStream()));
    String sl ="";
    String line;
    while ((line = brl.readLine()) != null) {
      System.out.println("LINES!:"+line);
      sl += line;
    }
    int lc = Integer.parseInt(sl);
    int slc = lc/splits;
    int splitsize = Math.floorMod(slc, 4);

    ArrayList<Integer> splitnum = new ArrayList<>();
    for (int i = 0; i<=splits; i++)
      splitnum.add(i);*/
}