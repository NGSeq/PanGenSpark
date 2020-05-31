package org.ngseq.pangenomics.tools;


import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**Usage
 * spark-submit --master local[15] --class fi.aalto.ngs.seqspark.metagenomics.Interleave target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/reads/reads/forward.fastq -fastq2 /user/root/reads/reads/reverse.fastq -splitsize 100000 -splitout /user/root/interleaved2 -interleave
 **/


public class Interleave {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  //TODO: change to not static, might cause exceptions with Yarn


  public Interleave() {
  }

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("Interleave");
    //conf.set("spark.scheduler.mode", "FAIR");
    //conf.set("spark.scheduler.allocation.file", "/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/etc/hadoop/conf.dist/pools.xml");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.setLocalProperty("spark.scheduler.pool", "production");

    //String query = args[2];

    Options options = new Options();
    /*Option pathOpt = new Option( "fastq", true, "Path to fastq file in hdfs." );
    Option path2Opt = new Option( "fastq2", true, "Path to fastq file in hdfs." );
    Option splitDirOpt = new Option( "out", true, "Path to output directory in hdfs." );
    Option numsplitsOpt = new Option( "splitsize", true, "Number of reads in split, depends on the size of read file, number of cores and available memory." );*/
    Option pairedOpt = new Option( "paired", "Split paired end reads to separate folders, does not interleave." );
    Option intOpt = new Option( "singlesplit", "" );
    options.addOption( new Option( "decompress", "" ) );

    /*options.addOption( pathOpt );
    options.addOption( path2Opt );
    options.addOption( numsplitsOpt );
    options.addOption( splitDirOpt );*/
    options.addOption( pairedOpt );
    options.addOption( intOpt );
    options.addOption(new Option( "help", "print this message" ));

    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp( "spark-submit <spark specific args>", options, true );

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
    /*
    String outdir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String fastq = cmd.getOptionValue("fastq");
    String fastq2 = (cmd.hasOption("fastq2")==true)? cmd.getOptionValue("fastq2"):null;
    int splitsize = (cmd.hasOption("splitsize")==true)? Integer.parseInt(cmd.getOptionValue("splitsize")):0;
    boolean paired = cmd.hasOption("paired");
    boolean singlesplit = cmd.hasOption("singlesplit");
    boolean decompress = cmd.hasOption("decompress");*/


    String fastq = args[0];
    String fastq2 = args[1];
    String outdir = args[2];
    int splitsize = Integer.valueOf(args[3]);
    boolean paired = cmd.hasOption("paired");
    boolean singlesplit = cmd.hasOption("singlesplit");
    boolean decompress = cmd.hasOption("decompress");

    String outdir2 = null;
    if(paired)
      outdir2 = outdir+"2";

    FileSystem fs = FileSystem.get(new Configuration());
    if(decompress){
      decompress(fs, fastq, "temp1.fq");
      decompress(fs, fastq2, "temp2.fq");

      fastq = "temp1.fq";
      fastq2 = "temp2.fq";

    }

      /*if(fastq.startsWith("file://")){
        //TODO: treat local files differently, move to hdfs or split by external commands
        File f = new File(fastq);
      }else{

      }*/
      //Count split positions
      int splitlen = splitsize*4; //FASTQ read is expressed by 4 lines

      if(singlesplit){
        FileStatus fstatus = fs.getFileStatus(new Path(fastq));
        //int lines = (int)(fstatus.getLen()/splitsize)/(8*550); //approximate one read has 150bp*3lines+100Bytes for name => 550Bytes/read
        splitFastq(fstatus, fastq, outdir, splitlen, sc);
        if(paired){
          FileStatus fstatus2 = fs.getFileStatus(new Path(fastq2));
          splitFastq(fstatus2, fastq2, outdir2, splitlen, sc);
        }
      }else{
        FileStatus fst = fs.getFileStatus(new Path(fastq));
        FileStatus fst2 = fs.getFileStatus(new Path(fastq2));
        //long len2 = fs.getFileStatus(new Path(fastq2)).getLen();
        //TODO: pipe per individual read dirs interleave-reads.py /media/data/genomes/reads/forward.fastq /media/data/genomes/reads/reverse.fastq -o interleaved.fq
        //OR
        //FIXME: interleaved splits does not match always..
        interleaveSplitFastq(fst, fst2, outdir, splitlen, sc);
      }

    if(decompress){
      fs.delete(new Path("temp1.fq"), false);
      fs.delete(new Path("temp2.fq"), false);
    }

    sc.stop();

  }

  private static void decompress(FileSystem fs, String in, String outpath) throws IOException {
    //TODO: handle compressed files, use LZO if possible as it is splittable format
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    // the correct codec will be discovered by the extension of the file

    CompressionCodec codec = factory.getCodec(new Path(in));
    //Decompressing zip file.
    InputStream is = codec.createInputStream(fs.open(new Path(in)));
    OutputStream out = fs.create(new Path(outpath));
    //Write decompressed out
    IOUtils.copyBytes(is, out, conf);
    is.close();
    out.close();
  }


  private static void splitFastq(FileStatus fst, String fqPath, String splitDir, int splitlen, JavaSparkContext sc) throws IOException {
    Path fqpath = new Path(fqPath);
    String fqname = fqpath.getName();
    String[] ns = fqname.split("\\.");
    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);

    splitRDD.foreach( split ->  {

      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), split);
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
      while(next) {
        Text t = fqreader.getCurrentKey();
        SequencedFragment sf = fqreader.getCurrentValue();
        next = fqreader.next(t, sf);

        if(next) writer.write(fqreader.getCurrentKey(), fqreader.getCurrentValue());

        Text t2 = fqreader2.getCurrentKey();
        SequencedFragment sf2 = fqreader2.getCurrentValue();
        fqreader2.next(t2, sf2);

        if(next) writer.write(fqreader2.getCurrentKey(), fqreader2.getCurrentValue());

      }

      dataOutput.close();
      os.close();

    } catch (IOException e) {
      e.printStackTrace();
    }
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

  public static void interleaveSplitFastq(FileStatus fst, FileStatus fst2, String splitDir, int splitlen, JavaSparkContext sc) throws IOException {

    String[] ns = fst.getPath().getName().split("\\.");
    //TODO: Handle also compressed files
    List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);
    List<FileSplit> nlif2 = NLineInputFormat.getSplitsForFile(fst2, sc.hadoopConfiguration(), splitlen);

    JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);
    JavaRDD<FileSplit> splitRDD2 = sc.parallelize(nlif2);
    JavaPairRDD<FileSplit, FileSplit> zips = splitRDD.zip(splitRDD2);

    zips.foreach( splits ->  {
      Path path = splits._1.getPath();
      FastqRecordReader fqreader = new FastqRecordReader(new Configuration(), splits._1);
      FastqRecordReader fqreader2 = new FastqRecordReader(new Configuration(), splits._2);
      writeInterleavedSplits(fqreader, fqreader2, new Configuration(), splitDir+"/"+path.getParent().getName()+"_"+splits._1.getStart()+".fq");
    });
  }

 private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long length) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, length, null);

    return new FastqRecordReader(config, split);
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