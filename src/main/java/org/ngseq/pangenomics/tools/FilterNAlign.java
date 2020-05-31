package org.ngseq.pangenomics.tools;


import com.github.lindenb.jbwa.jni.BwaIndex;
import com.github.lindenb.jbwa.jni.BwaMem;
import com.github.lindenb.jbwa.jni.ShortRead;
import htsjdk.samtools.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**Usage
 *  Usage example:
 spark-submit --master local[4] --class fi.aalto.ngs.seqspark.metagenomics.FilterNAlign target/metagenomics-0.9-jar-with-dependencies.jar -fastq fqchunks -out flt -ref /index/adhoc.fa -format fastq -unmapped

 spark-submit --master yarn --deploy-mode client --conf spark.dynamicAllocation.enabled=true --executor-memory 15g  --conf spark.yarn.executor.memoryOverhead=5000 --conf spark.task.maxFailures=100 --conf spark.yarn.max.executor.failures=100 --class fi.aalto.ngs.seqspark.metagenomics.FilterNAlign target/metagenomics-0.9-jar-with-dependencies.jar -fastq interleaved -out alignedfiltered -ref /index/hg38.fa -format fastq -unmapped

 NOTE: DO NOT PUT INDEX FILES UNDER ROOT FOLDER, or YARN can not access those
 **/


public class FilterNAlign {

  //Example run from examples/target dir
  //su hdfs
  //unset SPARK_JAVA_OPTS

  public static void main(String[] args) throws IOException {
    SparkConf conf = new SparkConf().setAppName("FilterNAlign");
    //conf.set("spark.scheduler.mode", "FAIR");
    //conf.set("spark.scheduler.allocation.file", "/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/etc/hadoop/conf.dist/pools.xml");
    JavaSparkContext sc = new JavaSparkContext(conf);
    //sc.setLocalProperty("spark.scheduler.pool", "production");

    //String query = args[2];

    Options options = new Options();
    Option pathOpt = new Option( "fastq", true, "Path to fastq file in hdfs." );
    Option path2Opt = new Option( "fastq2", true, "Path to fastq file in hdfs." );
    //gmOpt.setRequired(true);
    Option refOpt = new Option( "ref", true, "Path to fasta reference file." );

    Option splitOpt = new Option( "split", "Perform read file splitting" );
    Option alignOpt = new Option( "align", "Perform BWA alignment" );
    Option fqoutOpt = new Option( "out", true, "" );
    Option formatOpt = new Option( "format", true, "bam or sam, fastq is default" );
    Option umOpt = new Option( "unmapped", "keep unmapped, true or false. If not any given, all alignments/reads are persisted" );
    Option mappedOpt = new Option( "mapped", "keep mapped, true or false. If not any given, all alignments/reads are persisted" );

    options.addOption(new Option( "avgq",true, "Minimum value for quality average score" ));
    options.addOption(new Option( "lowqc", true, "Maximum count for low quality bases under threshold, lowqt must be given also."));
    options.addOption(new Option( "lowqt", true, "Threshold for low quality." ));

    options.addOption( pathOpt );
    options.addOption( path2Opt );
    options.addOption( splitOpt );
    options.addOption( refOpt );
    options.addOption( splitOpt );
    options.addOption( alignOpt );
    options.addOption( formatOpt );
    options.addOption( fqoutOpt );
    options.addOption( umOpt );
    options.addOption( mappedOpt );

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
    String fastq = cmd.getOptionValue("fastq");
    String fastq2 = (cmd.hasOption("fastq2")==true)? cmd.getOptionValue("fastq2"):null;
    String ref = (cmd.hasOption("ref")==true)? cmd.getOptionValue("ref"):null;
    boolean unmapped = cmd.hasOption("unmapped");
    boolean mapped = cmd.hasOption("mapped");
    String outDir = (cmd.hasOption("out")==true)? cmd.getOptionValue("out"):null;
    String format = (cmd.hasOption("format") == true) ? cmd.getOptionValue("format") : "bam";

    int minAvgQuality = (cmd.hasOption("avgq")==true)? Integer.parseInt(cmd.getOptionValue("avgq")):0;
    int maxLowQualityCount = (cmd.hasOption("lowqc")==true)? Integer.parseInt(cmd.getOptionValue("lowqc")):0;
    int qualityThreshold = (cmd.hasOption("lowqt")==true)? Integer.parseInt(cmd.getOptionValue("lowqt")):0;

      JavaPairRDD<Text, SequencedFragment> fastqRDD = sc.newAPIHadoopFile(fastq, FastqInputFormat.class, Text.class, SequencedFragment.class, sc.hadoopConfiguration());

      JavaRDD<String> alignmentRDD = fastqRDD.mapPartitions(split -> {
        //THIS MUST BE LOADED HERE IN YARN
        System.loadLibrary("bwajni");
        //TODO: Modify JBWA to use SAMRecord class, this woudl radically reduce map operations
        BwaIndex index = new BwaIndex(new File(ref));
        BwaMem mem = new BwaMem(index);

        List<ShortRead> L1 = new ArrayList<ShortRead>();
        List<ShortRead> L2 = new ArrayList<ShortRead>();

        //BUG OR FEATURE? org.apache.hadoop.io.Text.getBytes() gives different than Text.toString().getBytes(). USE Text.toString().getBytes()!!
        while (split.hasNext()) {
          Tuple2<Text, SequencedFragment> next = split.next();
          String key = next._1.toString();
          //FIXME: With BWA 0.7.12 new Illumina reads fail: [mem_sam_pe] paired reads have different names: "M02086:39:AA7PR:1:1101:6095:20076 1:N:0:1", "M02086:39:AA7PR:1:1101:6095:20076 2:N:0:1"
          String[] keysplit=key.split(" ");
          key = keysplit[0];

          SequencedFragment origsf = next._2;
          String quality = origsf.getQuality().toString();
          String sequence = origsf.getSequence().toString();
          SequencedFragment sf = copySequencedFragment(origsf, sequence, quality);

          //Apply quality filters if given
          if (maxLowQualityCount != 0) {
            if (!lowQCountTest(maxLowQualityCount, qualityThreshold, sf.getQuality().toString().getBytes())){
              split.next(); //We skip over read pair
              continue;
            }
          }
          if (minAvgQuality != 0) {
            if (!avgQualityTest(minAvgQuality, sf.getQuality().toString().getBytes())) {
              split.next(); //We skip over read pair
              continue;
            }
          }

          if(split.hasNext()){

            Tuple2<Text, SequencedFragment> next2 = split.next();
            String key2 = next2._1.toString();
            String[] keysplit2=key2.split(" ");
            key2 = keysplit2[0];
            //key2 = key2.replace(" 2:N:0:1","/2");

            SequencedFragment origsf2 = next2._2;
            String quality2 = origsf2.getQuality().toString();
            String sequence2 = origsf2.getSequence().toString();
            SequencedFragment sf2 = copySequencedFragment(origsf2, sequence2, quality2);

            //Apply quality filters if given
            if (maxLowQualityCount != 0) {
              if (!lowQCountTest(maxLowQualityCount, qualityThreshold, sf2.getQuality().toString().getBytes()))
                continue;
            }
            if (minAvgQuality != 0) {
              if (!avgQualityTest(minAvgQuality, sf2.getQuality().toString().getBytes()))
                continue;
            }

            if(key.equalsIgnoreCase(key2)){
              L1.add(new ShortRead(key, sf.getSequence().toString().getBytes(), sf.getQuality().toString().getBytes()));
              L2.add(new ShortRead(key2, sf2.getSequence().toString().getBytes(), sf2.getQuality().toString().getBytes()));
            }else
              split.next();

          }
        }

        String[] aligns = mem.align(L1, L2);

        if (aligns != null) {

              ArrayList<String> filtered = new ArrayList<String>();
              if(mapped==true){
                Arrays.asList(aligns).forEach(aln -> {
                  //FIXME: Something wrong with big data sets, probably when index contains multiple chromosomes and parser fails with mapped reads, htsjdk.samtools.SAMRecord.resolveIndexFromName(SAMRecord.java:528)  ERROR chromosome not found
                  //TODO: fix by checking values from alignment string or fix parsing
                  final SAMLineParser samLP = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.SILENT,  new SAMFileHeader(), null, null);
                  //String flag = fields[1];
                  SAMRecord record = samLP.parseLine(aln);
                   //We want only mapped reads
                    if(!record.getReadUnmappedFlag()){
                      filtered.add(aln);
                    }

                });
                return filtered.iterator();
              }
              else if(unmapped==true){
                Arrays.asList(aligns).forEach(aln -> {
                  final SAMLineParser samLP = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.SILENT,  new SAMFileHeader(), null, null);
                  //String flag = fields[1];
                  SAMRecord record = samLP.parseLine(aln);
                  //We want only unmapped reads
                  if(record.getReadUnmappedFlag()){
                    filtered.add(aln);
                  }
                });
                return filtered.iterator();
              }
              else{
                return Arrays.asList(aligns).iterator(); //NO MAPPING FILTER
              }
        } else
          return new ArrayList<String>().iterator(); //NULL ALIGNMENTS

      });

    //alignmentRDD.saveAsTextFile("test");
    //filteredRDD.count();

    if(format!=null)
      new HDFSWriter(alignmentRDD, outDir, format, sc);
    else
      alignmentRDD.saveAsTextFile(outDir);

    sc.stop();

  }

  private static boolean avgQualityTest(double minAvgQuality, byte[] bytes) {
    int qSum = 0;
    for(byte b : bytes){
      qSum+=b;
    }
    if((qSum/bytes.length) > minAvgQuality)
      return true;

    return false;
  }

  private static boolean lowQCountTest(int maxLowQualityCount, int qualityThreshold, byte[] bytes) {
    int lowqCount = 0;
    for(byte b : bytes){
      if(b<qualityThreshold)
        lowqCount++;
    }
    if(lowqCount < maxLowQualityCount)
      return true;

    return false;
  }

  private static SequencedFragment copySequencedFragment(SequencedFragment sf, String sequence, String quality) {
    SequencedFragment copy = new SequencedFragment();

    copy.setControlNumber(sf.getControlNumber());
    copy.setFilterPassed(sf.getFilterPassed());
    copy.setFlowcellId(sf.getFlowcellId());
    copy.setIndexSequence(sf.getIndexSequence());
    copy.setInstrument(sf.getInstrument());
    copy.setLane(sf.getLane());
    copy.setQuality(new Text(quality));
    copy.setRead(sf.getRead());
    copy.setRunNumber(sf.getRunNumber());
    copy.setSequence(new Text(sequence));
    copy.setTile(sf.getTile());
    copy.setXpos(sf.getXpos());
    copy.setYpos(sf.getYpos());

    return copy;
  }


}