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
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.FastqInputFormat.FastqRecordReader;
import org.seqdoop.hadoop_bam.FastqOutputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

// $example on$
//spark-submit  --master local[6] --driver-memory 6g --executor-memory 3g --class fi.aalto.ngs.seqspark.metagenomics.ImportFastq target/metagenomics-0.9-jar-with-dependencies.jar -fastq reverse.fastq -out fastq/reverse -splits 100
//spark-submit  --master yarn-client --num-executors 6 --driver-memory 2g --driver-memory 6g --executor-memory 3g --class fi.aalto.ngs.seqspark.metagenomics.ImportFastq target/metagenomics-0.9-jar-with-dependencies.jar -fastq reverse.fastq -out fastq/reverse -splits 100

// $example off$

public class ImportFastq {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("ImportFastq");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(sc);

    //String query = args[2];

    Options options = new Options();
    Option pathOpt = new Option( "fastq", true, "Path to fastq files." );

    Option splitDirOpt = new Option( "out", true, "Path to output directory in hdfs." );

    Option prefixOpt = new Option( "prefix", true, "Prefix of input haplotype files. Usually string before .phased extension without chromosome number." );

    Option splitsOpt = new Option( "splitsize", true, "Number of reads in splits, depends on the size of read file, number of cores and available memory." );
    //gmOpt.setRequired(true);
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );

    Option opOpt = new Option( "bwaout", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option zippedOpt = new Option( "zipped", "" );

    options.addOption( pathOpt );
    options.addOption( prefixOpt );
    options.addOption( splitsOpt );
    options.addOption( refOpt );
    options.addOption( opOpt );
    options.addOption( zippedOpt );
    options.addOption( splitDirOpt );


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
    String outpath = cmd.getOptionValue("out");
    String fastq = cmd.getOptionValue("fastq");
    int splitsize = (cmd.hasOption("splitsize")==true)? Integer.parseInt(cmd.getOptionValue("splitsize")):0;

    FileSystem fs = FileSystem.get(new Configuration());

    //Zipping needs same amount of partitions in Alignment phase, so use same split number

    if(cmd.hasOption("zipped")){
      //TODO: uncompress from local FS also
      /*CompressionCodecFactory factory = new CompressionCodecFactory(sc.hadoopConfiguration());
      // the correct codec will be discovered by the extension of the file
      CompressionCodec codec = factory.getCodec(new Path(fastq));
      String outputUri =  CompressionCodecFactory.removeSuffix(outpath, codec.getDefaultExtension());
      //Decompressing zip file
      InputStream is = codec.createInputStream(fs.open(new Path(fastq)), codec.createDecompressor());
      OutputStream out = fs.create(new Path(outputUri));
      CompressionOutputStream osr = codec.createOutputStream(out);
      //FIXME: does not save uncompressed file
      //Write decompressed out
      IOUtils.copyBytes(is, osr, sc.hadoopConfiguration());*/
      JavaRDD<String> zipFileRDD = sc.textFile(fastq);
      JavaRDD<String> contentRDD = zipFileRDD.map(s -> new String(s.getBytes()));

      contentRDD.saveAsTextFile(outpath);
    }else{
      //TODO: MOdify Hadoop-BAM FastqInputFormat so that paired fastqs can be splitted by lines, USE NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen) instead of FileInputFormat
      JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());
      //TODO: Paired-end reads must have same amount of partitions if zipping is used in the alignment phase
      //fastqRDD.groupByKey(100);
      //fastqRDD.repartition(100);
      FileStatus fstatus = fs.getFileStatus(new Path(fastq));
      List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fstatus, sc.hadoopConfiguration(), splitsize);

      //JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif).mapToPair();

     /* splitRDD.foreach( split ->  {
        //fastqRDD.saveAsNewAPIHadoopFile(outpath, Record.class, RecordWritable.class, FileOutputFormat.class, sc.hadoopConfiguration());

      });*/


    }

    //JavaRDD<String> fastqFileRDD = sc.textFile(fastq);
    //fastqFileRDD.saveAsTextFile(outpath);

    //

    sc.stop();

  }

  private static FastqRecordReader readFastqSplits(String fastqfile, Configuration config, long start, long splitlen) throws IOException {

    FileSplit split = new FileSplit(new Path(fastqfile), start, splitlen, null);

    return new FastqRecordReader(config, split);
  }

  private static void writeFastqSplits(FastqRecordReader fqreader , Configuration config, String fileName){

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
      Text t = new Text();
      SequencedFragment sf = new SequencedFragment();
      boolean next = true;
      while(next) {
        t = fqreader.getCurrentKey();
        sf = fqreader.getCurrentValue();
        writer.write(fqreader.getCurrentKey(), fqreader.getCurrentValue());
        next = fqreader.next(t, sf);
        System.out.println("Written READ:  "+ t.toString()+"   SEQ  "+sf.getSequence());
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