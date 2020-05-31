package org.ngseq.pangenomics.tools;

import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Created by Altti Ilari Maarala on 7/20/16.
 */
public class VCFSplitter {
    //private static VCFHeader header;

    //Executing with Spark locally
    //spark-submit --master local[4] --class org.apache.spark.genomics.sbeagle.BeagleSparkInterface target/seqspark-1.6.1-jar-with-dependencies.jar /media/data/dev/spark-1.6.1/seqspark/data/test.27Jul16.86a.vcf.gz out.gt
    /***
     *
     * ***/


    public static void main(String[] args) throws IOException {


        SparkConf sparkconf = new SparkConf().setAppName("VCFSplitter");
        JavaSparkContext sc = new JavaSparkContext(sparkconf);
        //SQLContext sqlContext  = new HiveContext(sc);
        HiveContext sqlContext = new HiveContext(sc.sc());

        Options options = new Options();
        Option pathOpt = new Option( "path", true, "Path to local working directory where all input files are stored." );
        pathOpt.setRequired(true);
        Option prefixOpt = new Option( "file", true, "Prefix of input haplotype files. Usually string before .phased extension without chromosome number." );
        prefixOpt.setRequired(true);
        Option chrOpt = new Option( "chr", true, "Chromosome number must be given." );
        //chrOpt.setRequired(true);
        Option s = new Option( "s", true, "Chromosomal start position of interested region" );
        s.setRequired(true);
        Option e = new Option( "e", true, "Chromosomal end position of interested region" );
        e.setRequired(true);
        Option intOpt = new Option( "rec", true, "Number of Records in split" );
        intOpt.setRequired(true);
        Option neOpt = new Option( "ol", true, "Windows overlap size" );
        neOpt.setRequired(true);
        Option gmOpt = new Option( "gm", true, "Path to genetic map file" );
        //gmOpt.setRequired(true);
        Option opOpt = new Option( "op", true, "HDFS path for output files. If not present, the output files are not moved to HDFS." );

        options.addOption( pathOpt );
        options.addOption( prefixOpt );
        options.addOption( chrOpt );
        options.addOption( s );
        options.addOption( e );
        options.addOption( intOpt );
        options.addOption( neOpt );
        options.addOption( gmOpt );
        options.addOption( opOpt );

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
        String workingDir = cmd.getOptionValue("path");
        String inputPrefix = cmd.getOptionValue("file");
        String chr = cmd.getOptionValue("chr");
        String op = (cmd.hasOption("op")==true)? cmd.getOptionValue("op"):null;
        int startChr = Integer.parseInt(cmd.getOptionValue("s"));
        int endChr = Integer.parseInt(cmd.getOptionValue("e"));
        long records = Long.parseLong(cmd.getOptionValue("rec"));
        long overlap = Long.parseLong(cmd.getOptionValue("ol"));

        ArrayList<Integer> chromosomes = new ArrayList<Integer>();
        for (int i = startChr; i <= endChr; i++)
            chromosomes.add(i);
        //Get splitted files

        String inputFN = workingDir+"/"+inputPrefix;
        //TODO: optionally parallelize by chromosome positions/intervals
        JavaRDD<Integer> posRDD = sc.parallelize(chromosomes);

        JavaRDD<String> outRDD = posRDD.map(chrom -> {

              //vcf-subset -c HG00171 ALL.chr17.phase1_release_v3.20101123.snps_indels_svs.genotypes.vcf.gz | bgzip -c > HG00171.vcf.gz
            
                //zcat data/test.27Jul16.86a.vcf.gz | java -jar beagle/splitvcf.jar 22:20000000-20074699 100 5 ./split
              Process process = new ProcessBuilder("/bin/sh", "-c", "hdfs dfs -text "+inputFN+" | java -jar /beagle/splitvcf.jar "+chrom+" "+records+" "+overlap+" /mnt/ramdisk/split_"+chrom).start();
              InputStream is = process.getInputStream();
              InputStreamReader isr = new InputStreamReader(is);
              BufferedReader br = new BufferedReader(isr);

              String out ="";
              String line;
              while ((line = br.readLine()) != null) {
                System.out.println(line);
                out += line;
              }

            if(op!=null) {
                //TODO: check that files are really moved
                new ProcessBuilder("/bin/sh", "-c", "hdfs dfs -moveFromLocal /tmp/split_"+chrom+".*.vcf.gz " + op ).start();
            }
            //genotyping(fname, out);
            return out;
        });

        System.out.println(outRDD.collect());
    }

    RecordReader<LongWritable, VariantContextWritable> readFromLocalFSHadoopStyle(Configuration conf, String inputfile) throws IOException {
        conf.set("hadoopbam.vcf.trust-exts", "true");
        conf.set("mapreduce.input.fileinputformat.inputdir", inputfile);
        TaskAttemptContext taskAttemptContext = ContextUtil.newTaskAttemptContext(conf, mock(TaskAttemptID.class));
        JobContext ctx = ContextUtil.newJobContext(conf, taskAttemptContext.getJobID());
        VCFInputFormat inputFormat = new VCFInputFormat(conf);
        List<InputSplit> splits = inputFormat.getSplits(ctx);
        RecordReader<LongWritable,VariantContextWritable> reader = null;
        try {
            reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);
            reader.initialize(splits.get(0), taskAttemptContext);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return reader;
    }

    void writeToLocalFs(Configuration conf, String headerInputfile, String outputfile, RecordReader<LongWritable,VariantContextWritable> reader) throws IOException {
        TaskAttemptContext taskAttemptContext = ContextUtil.newTaskAttemptContext(conf, mock(TaskAttemptID.class));
        JobContext ctx = ContextUtil.newJobContext(conf, taskAttemptContext.getJobID());
        KeyIgnoringVCFOutputFormat<Long> vcfOutputFormat = new KeyIgnoringVCFOutputFormat(VCFFormat.VCF);

        VCFHeader vcfheader = VCFHeaderReader.readHeaderFrom(new SeekableFileStream(new File(headerInputfile)));
        vcfOutputFormat.setHeader(vcfheader);

        VariantContextWritable vcw = new VariantContextWritable();
        RecordWriter<Long, VariantContextWritable> vcWriter = vcfOutputFormat.getRecordWriter(taskAttemptContext, new Path("vcfW_"+outputfile));
        try {
            vcw = reader.getCurrentValue();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        VariantContext vc = vcw.get();
        vcw.set(vc);
        try {
            vcWriter.write(1L, vcw);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



}
