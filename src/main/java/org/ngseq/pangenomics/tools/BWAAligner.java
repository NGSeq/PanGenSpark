package org.ngseq.pangenomics.tools;


import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import htsjdk.samtools.SAMFileHeader;
import org.apache.commons.cli.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**Usage
 *  spark-submit --master local[4] --class fi.aalto.ngs.seqspark.metagenomics.BWAAligner target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/fqsplits -ref /index/adhoc.fa -bwaout /user/root/testb

 spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.enabled=true --executor-memory 10g  --conf spark.yarn.executor.memoryOverhead=3000 --conf spark.task.maxFailures=40 --conf spark.yarn.max.executor.failures=100 --conf spark.hadoop.validateOutputSpecs=true --class fi.aalto.ngs.seqspark.metagenomics.BWAAligner target/metagenomics-0.9-jar-with-dependencies.jar -fastq interleaved -ref /index/hg38.fa -bwaout bam -align -unmapped

 20G of memory/executor seems to be enough for 100 to 150MB fastq chunks and 3G reference index
 Took 1hr15min min to align 10GB reads to HG38 reference genome with 5 nodes 80GB RAM and 16cores in each
 There's still load balancing problems
 More mapped reads => longer running time, 47 min was taken when all but 2 reads were mapped (partition 93)
 Seems that mapped reads are collected in same partition(93)?
 => it's better to do filtering together with parsing stage
 Metric	    Min	    25th percentile	  Median	75th percentile	    Max
 Duration	52 s	1.5 min	          1.7 min	2.1 min	            47 min
 http://aim-nn.novalocal:18088/history/application_1486571309040_0005

 spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.enabled=false --num-executors 100 --executor-memory 20g  --conf spark.yarn.executor.memoryOverhead=3000 --conf spark.task.maxFailures=40 --conf spark.yarn.max.executor.failures=100 --conf spark.hadoop.validateOutputSpecs=true --class fi.aalto.ngs.seqspark.metagenomics.BWAAligner target/metagenomics-0.9-jar-with-dependencies.jar -fastq interleaved -ref /index/hg38.fa -bwaout bam -align

 **/


public class BWAAligner {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  private static String alignmentTable = "alignedreads";
  private static String rawreadsTable = "rawreads";
  private static long splitlen;
  private static String format = "sam";
  private static boolean filterunmapped = false;


  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("BWAAligner");
    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new HiveContext(sc.sc());

    //String query = args[2];

    Options options = new Options();
    Option pathOpt = new Option( "fastq", true, "Path to fastq file in hdfs." );
    Option path2Opt = new Option( "fastq2", true, "Path to fastq file in hdfs." );
    Option splitDirOpt = new Option( "splitout", true, "Path to output directory in hdfs." );
    Option prefixOpt = new Option( "prefix", true, "Prefix of input haplotype files. Usually string before .phased extension without chromosome number." );
    Option numsplitsOpt = new Option( "splitsize", true, "Number of read splits, depends on the size of read file, number of cores and available memory." );
    //gmOpt.setRequired(true);
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );
    Option opOpt = new Option( "out", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option pairedOpt = new Option( "paired", "Use paired end reads" );

    Option queryOpt = new Option( "query", true, "SQL query string." );

    Option splitOpt = new Option( "split", "Perform read file splitting" );
    Option alignOpt = new Option( "align", "Perform BWA alignment" );
    Option samOpt = new Option( "samopts", true, "Options for samtools view given within parenthesis e.g.'-F 4'" );
    Option filterOpt = new Option( "unmapped", "" );
    Option formatOpt = new Option( "format", true, "bam or sam, bam is default" );

    options.addOption( pathOpt );
    options.addOption( path2Opt );
    options.addOption( prefixOpt );
    options.addOption( splitOpt );
    options.addOption( numsplitsOpt );
    options.addOption( refOpt );
    options.addOption( opOpt );
    options.addOption( splitDirOpt );
    options.addOption( pairedOpt );
    options.addOption( queryOpt );
    options.addOption( splitOpt );
    options.addOption( alignOpt );
    options.addOption( samOpt );
    options.addOption( filterOpt );
    options.addOption( formatOpt );
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
    String ref = (cmd.hasOption("ref")==true)? cmd.getOptionValue("ref"):null;
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;

    format = (cmd.hasOption("format")==true)? cmd.getOptionValue("format"):null;

    //TODO: handle compressed files, use LZO if possible as it is splittable format

      JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

      JavaRDD<String> alignmentRDD = fastqRDD.mapPartitions(split -> {
        //THIS MUST BE LOADED HERE IN YARN
        System.loadLibrary("bwajni");
        BwaIndex index = new BwaIndex(new File(ref));
        BwaMem mem = new BwaMem(index);

        ArrayList<ShortRead> L1 = new ArrayList<ShortRead>();
        ArrayList<ShortRead> L2 = new ArrayList<ShortRead>();
        int count = 0;
        while (split.hasNext()) {
          Tuple2<Text, SequencedFragment> next = split.next();
          String key = next._1.toString();
          //Some fastq formats have spaces in id field and Hadoop-BAM treats it as whole id
          String[] keysplit=key.split(" ");
          key = keysplit[0];
          SequencedFragment sf = next._2;

          //System.out.println(key+" seq1: "+ sf.getSequence().toString());
          //BUG OR WHAT? org.apache.hadoop.io.Text.getBytes() gives different than Text.toString().getBytes(). USE Text.toString().getBytes()!!
          if(count % 2==0)L1.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));
          else L2.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));

          count++;
        }

        String[] aligns = mem.align(L1, L2);
        if (aligns != null) {
          List<String> als = Arrays.asList(aligns);
          //als.forEach(a->System.out.print(a));
          return als.iterator();
        } else
          return new ArrayList<String>().iterator();

      });

    if(format!=null)
      new HDFSWriter(alignmentRDD, outDir, format, sc);
    else
      alignmentRDD.saveAsTextFile(outDir);

    sc.stop();

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

}