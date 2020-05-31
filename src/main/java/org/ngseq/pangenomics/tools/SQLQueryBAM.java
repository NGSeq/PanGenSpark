package org.ngseq.pangenomics.tools;

import htsjdk.samtools.SAMRecord;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**THIS IS IN MEMORY IMPLEMENTATION OF PARALLEL BWA AND READ FILTERING, NO READ SPLIT FILES ARE WRITTEN
 * Usage
 spark-submit  --master local[4] --driver-memory 4g --executor-memory 4g --class fi.aalto.ngs.seqspark.metagenomics.ParallelJBWANoSplit target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/forward.fastq.gz -fastq2 /user/root/reverse.fastq.gz -ref /media/data/genomes/hg38/hg38.fa -bwaout /user/root/ramdisk/bwa -query "SELECT * from alignedreads where readUnmapped=true" -numsplits 1000 -align

 spark-submit  --master local[4] --driver-memory 4g --executor-memory 4g --class fi.aalto.ngs.seqspark.metagenomics.ParallelJBWANoSplit target/metagenomics-0.9-jar-with-dependencies.jar -fastq /user/root/forward.fastq.gz -fastq2 /user/root/reverse.fastq.gz -ref /media/data/genomes/hg38/hg38.fa -bwaout /user/root/ramdisk/bwa -numsplits 1000 -align

 **/


public class SQLQueryBAM {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  private static String tablename = "records";

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("SQLQueryBAM");
    //conf.set("spark.executor.instances","8");
    //conf.set("spark.dynamicAllocation.enabled", "false");
    //conf.set("spark.shuffle.service.enabled", "true");

    JavaSparkContext sc = new JavaSparkContext(conf);
    SQLContext sqlContext = new HiveContext(sc.sc());

    //String query = args[2];

    Options options = new Options();
    //gmOpt.setRequired(true);

    Option opOpt = new Option( "bwaout", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );
    Option queryOpt = new Option( "query", true, "SQL query string." );
    Option baminOpt = new Option( "bamin", true, "" );
    options.addOption( new Option( "tablename", true, "Default sql table name is 'records'"));

    options.addOption( opOpt );
    options.addOption( queryOpt );
    options.addOption( baminOpt );
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

    String bwaOutDir = (cmd.hasOption("bwaout")==true)? cmd.getOptionValue("bwaout"):null;
    String query = (cmd.hasOption("query")==true)? cmd.getOptionValue("query"):null;
    String bamin = (cmd.hasOption("bamin")==true)? cmd.getOptionValue("bamin"):null;
    tablename = (cmd.hasOption("tablename")==true)? cmd.getOptionValue("tablename"):"records";

    sc.hadoopConfiguration().setBoolean(BAMInputFormat.KEEP_PAIRED_READS_TOGETHER_PROPERTY, true);

    //Read BAM/SAM from HDFS

    JavaPairRDD<LongWritable, SAMRecordWritable> bamPairRDD = sc.newAPIHadoopFile(bamin, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());
    //Map to SAMRecord RDD
    JavaRDD<SAMRecord> samRDD = bamPairRDD.map(v1 -> v1._2().get());
    JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag()));

    Dataset<Row> samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
    samDF.registerTempTable(tablename);
    if(query!=null) {

      //JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag(), bam));
      //Save as parquet file
      Dataset df2 = sqlContext.sql(query);
      df2.show(100,false);

      if(bwaOutDir!=null)
        df2.write().parquet(bwaOutDir);

    }else{
      if(bwaOutDir!=null)
        samDF.write().parquet(bwaOutDir);
    }

    sc.stop();

  }


}