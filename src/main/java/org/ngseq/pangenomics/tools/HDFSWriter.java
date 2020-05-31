package org.ngseq.pangenomics.tools;

import htsjdk.samtools.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.*;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import scala.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Altti Ilari Maarala on 7/20/16.
 */

public class HDFSWriter {

    public static boolean filterunmapped = false;
    public static String reference = "";
    public static String alignmentTable = "alignments";
    //public static String samheader =  "@HD\\tVN:0.7.15-r1140\\tSO:coordinate\\n@SQ\\tSN:chr1\\n@SQ\\tSN:chr2\\n@SQ\\tSN:chr3";
    public static String samheader =  "@HD\\tVN:0.7.15-r1140";


    private JavaSparkContext sparkcontext;

    public JavaPairRDD<LongWritable, SAMRecordWritable> getRecords() {
        return records;
    }

    private final JavaPairRDD<LongWritable, SAMRecordWritable> records = null;

    public HDFSWriter(JavaSparkContext sc) throws IOException {

        this.sparkcontext = sc;

    }

    public HDFSWriter(JavaSparkContext sc, String inputpath, boolean broadcastHeader) throws IOException {

        //If we want to distribute BAM file to HDFS, we read header from original BAM file and broadcast it

        if(broadcastHeader){
            SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(new Path(inputpath), sc.hadoopConfiguration());
            final Broadcast<SAMFileHeader> headerBc = sc.broadcast(header);
        }

        //records = sc.newAPIHadoopFile(inputpath, BAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());

        //print results
        /*for (Row r: result.collect()) {
            System.out.println("readgroup: "+ r.getString(0));
            System.out.println("start: " +r.getInt(1));
            System.out.println("length: " +r.getInt(2));
            System.out.println("bases: " +r.getString(3));
            System.out.println("cigar: " +r.getString(4));
        }*/

    }

    public HDFSWriter(JavaRDD<String> alignmentRDD, String outpuDir, String format, JavaSparkContext sc) throws IOException {

        SAMFileHeader header = new SAMFileHeader();
        //header.setTextHeader(alignments.take(1).get(0).split("\\t")[0].split(":")[0]);
        header.setTextHeader(samheader);

        if(format.equals("bam")||format.equals("sam")){

            //FIXME load balancing problem, some partitions can be larger size, could help if read splitting makes equal size partitions
            if(format.equals("bam")){
                final Broadcast<SAMFileHeader> headerBc = sc.broadcast(header);
                BAMHeaderOutputFormat.setHeader(headerBc.getValue());

                JavaRDD<SAMRecord>  samRDD = alignmentsToSAM(alignmentRDD, header);
                //Map and save

                JavaRDD<SAMRecord> samHRDD = setPartitionHeaders(samRDD, headerBc);
                JavaPairRDD<SAMRecord, SAMRecordWritable> samWritableRDD = readsToWritable(samHRDD, headerBc);
                samWritableRDD.saveAsNewAPIHadoopFile(outpuDir, BAMRecord.class, SAMRecordWritable.class, BAMHeaderOutputFormat.class, sc.hadoopConfiguration());
            }
            if(format.equals("sam")){
                /*JavaRDD<SAMRecord>  samRDD = alignmentsToSAM(alignmentRDD);
                sc.hadoopConfiguration().setBoolean(KeyIgnoringAnySAMOutputFormat.WRITE_HEADER_PROPERTY, false);
                sc.hadoopConfiguration().set(AnySAMOutputFormat.OUTPUT_SAM_FORMAT_PROPERTY, "sam");
                SAMFileHeader header = new SAMFileHeader();
                //header.setTextHeader(alignments.take(1).get(0).split("\\t")[0].split(":")[0]);
                header.setTextHeader("@HD\\tVN:0.7.15-r1140\\tSO:unknown\\n@SQ\\tSN:"+reference+"");
                final Broadcast<SAMFileHeader> headerBc = sc.broadcast(header);

                BAMHeaderOutputFormat.setHeader(headerBc.getValue());
                JavaRDD<SAMRecord> samHRDD = setPartitionHeaders(samRDD, headerBc);
                JavaPairRDD<SAMRecord, SAMRecordWritable> samWritableRDD = readsToWritable(samHRDD);
                samWritableRDD.saveAsNewAPIHadoopFile(outpuDir, SAMRecord.class, SAMRecordWritable.class, BAMHeaderOutputFormat.class, sc.hadoopConfiguration());*/
                //Just for testing, doesn't write header

                alignmentRDD.saveAsTextFile(outpuDir);
            }
        }

        if(format.equals("fastq")){
            //if(filterunmapped)
            //fastqRDD.filter()
            JavaPairRDD<Text, SequencedFragment> newfastqRDD = alignmentsToFastq(alignmentRDD);
            newfastqRDD.saveAsNewAPIHadoopFile(outpuDir, Text.class, SequencedFragment.class, FastqOutputFormat.class, sc.hadoopConfiguration());
        }
        if(format.equals("parquet")){
            JavaRDD<SAMRecord>  samRDD = alignmentsToSAM(alignmentRDD, header);
            JavaRDD<MyAlignment> rdd = samRDD.map(bam -> new MyAlignment(bam.getReadName(), bam.getStart(), bam.getReferenceName(), bam.getReadLength(), new String(bam.getReadBases(), StandardCharsets.UTF_8), bam.getCigarString(), bam.getReadUnmappedFlag(), bam.getDuplicateReadFlag()));

            SQLContext sqlContext = new SQLContext(sc.sc());
            Dataset samDF = sqlContext.createDataFrame(rdd, MyAlignment.class);
            samDF.registerTempTable(alignmentTable);
            samDF.write().parquet(outpuDir);
        }

    }

    private static JavaRDD<SAMRecord> alignmentsToSAM(JavaRDD<String> alignmentRDD, SAMFileHeader header) {
        return alignmentRDD.mapPartitions(alns -> {

            //TODO:If validation used, parsing fails,check alignment fields, reference name is wrong
            List<SAMRecord> records = new ArrayList<SAMRecord>();

            final SAMLineParser samLP = new SAMLineParser(new DefaultSAMRecordFactory(), ValidationStringency.SILENT, header, null, null);
            while (alns.hasNext()) {

                String aln = alns.next().replace("\r\n", "").replace("\n", "").replace(System.lineSeparator(), "");
                SAMRecord record = null;
                try{

                    /*String referenceName = aln.split("\\t")[2];
                    if(header.getValue().getSequenceDictionary()==null) header.getValue().setSequenceDictionary(new SAMSequenceDictionary());
                    if(header.getValue().getSequenceDictionary().getSequence(referenceName)==null)
                        header.getValue().getSequenceDictionary().addSequence(new SAMSequenceRecord(referenceName));

                    record = samLP.parseLine(aln);
                    record.setHeaderStrict(header.getValue());*/
                    record = samLP.parseLine(aln);
                    records.add(record);
                }catch(SAMFormatException e){
                    System.out.println(e.getMessage().toString());
                }
            }
            return records.iterator();
        });
    }

    private static JavaPairRDD<Text, SequencedFragment> alignmentsToFastq(JavaRDD<String> alignmentRDD) {
        return alignmentRDD.mapPartitionsToPair(alns -> {

            List<Tuple2<Text, SequencedFragment>> records = new ArrayList<Tuple2<Text, SequencedFragment>>();
            while (alns.hasNext()) {
                String aln = alns.next().replace("\r\n", "").replace("\n", "").replace(System.lineSeparator(), "");
                try{
                    String[] fields = aln.split("\\t");
                    String name = fields[0];
                    String flag = fields[1];
                    String bases = fields[9];
                    String quality = fields[10];

                    Text t = new Text(name);
                    SequencedFragment sf = new SequencedFragment();
                    sf.setSequence(new Text(bases));
                    sf.setQuality(new Text(quality));
                    records.add(new Tuple2<Text, SequencedFragment>(t, sf));
                }catch(SAMFormatException e){
                    System.out.println(e.getMessage().toString());
                }
            }
            return records.iterator();
        });
    }

    private static JavaPairRDD<Text, SequencedFragment> interleaveReads(String fastq, String fastq2, int splitlen, JavaSparkContext sc) throws IOException {

        FileSystem fs = FileSystem.get(new Configuration());

        FileStatus fst = fs.getFileStatus(new Path(fastq));
        FileStatus fst2 = fs.getFileStatus(new Path(fastq2));

        List<FileSplit> nlif = NLineInputFormat.getSplitsForFile(fst, sc.hadoopConfiguration(), splitlen);
        List<FileSplit> nlif2 = NLineInputFormat.getSplitsForFile(fst2, sc.hadoopConfiguration(), splitlen);

        JavaRDD<FileSplit> splitRDD = sc.parallelize(nlif);
        JavaRDD<FileSplit> splitRDD2 = sc.parallelize(nlif2);
        JavaPairRDD<FileSplit, FileSplit> zips = splitRDD.zip(splitRDD2);

        return zips.flatMapToPair( splits ->  {

            FastqInputFormat.FastqRecordReader fqreader = new FastqInputFormat.FastqRecordReader(new Configuration(), splits._1);
            FastqInputFormat.FastqRecordReader fqreader2 = new FastqInputFormat.FastqRecordReader(new Configuration(), splits._2);

            ArrayList<Tuple2<Text, SequencedFragment>> reads = new ArrayList<Tuple2<Text, SequencedFragment>>();
            while (fqreader.nextKeyValue()) {
                String key = fqreader.getCurrentKey().toString();
                //FIXME: With BWA 0.7.12 new Illumina reads fail: [mem_sam_pe] paired reads have different names: "M02086:39:AA7PR:1:1101:6095:20076 1:N:0:1", "M02086:39:AA7PR:1:1101:6095:20076 2:N:0:1"
                String[] keysplit = key.split(" ");
                key = keysplit[0];

                SequencedFragment sf = new SequencedFragment();
                sf.setQuality(new Text(fqreader.getCurrentValue().getQuality().toString()));
                sf.setSequence(new Text(fqreader.getCurrentValue().getSequence().toString()));

                if (fqreader2.nextKeyValue()) {

                    String key2 = fqreader2.getCurrentKey().toString();
                    String[] keysplit2 = key2.split(" ");
                    key2 = keysplit2[0];
                    //key2 = key2.replace(" 2:N:0:1","/2");

                    SequencedFragment sf2 = new SequencedFragment();
                    sf2.setQuality(new Text(fqreader2.getCurrentValue().getQuality().toString()));
                    sf2.setSequence(new Text(fqreader2.getCurrentValue().getSequence().toString()));
                    reads.add(new Tuple2<Text, SequencedFragment>(new Text(key), sf));
                    reads.add(new Tuple2<Text, SequencedFragment>(new Text(key2), sf2));
                }
            }

            return reads.iterator();

        });
    }

    private static JavaPairRDD<Text, SequencedFragment> mapSAMRecordsToFastq(JavaRDD<SAMRecord> bamRDD) {

        //Map SAMRecords to MyReads
        JavaPairRDD<Text, SequencedFragment> fastqRDD = bamRDD.mapToPair(read -> {

            String name = read.getReadName();
            if(read.getReadPairedFlag()){
                if(read.getFirstOfPairFlag())
                    name = name+"/1";
                if(read.getSecondOfPairFlag())
                    name = name+"/2";
            }

            //TODO: check values
            Text t = new Text(name);
            SequencedFragment sf = new SequencedFragment();
            sf.setSequence(new Text(read.getReadString()));
            sf.setQuality(new Text(read.getBaseQualityString()));

            return new Tuple2<Text, SequencedFragment>(t, sf);
        });
        return fastqRDD;
    }

    /*public void writeFastq(JavaPairRDD<Text, SequencedFragment> fastqRDD, String fastqOutDir) {

        fastqRDD.saveAsNewAPIHadoopFile(fastqOutDir, Text.class, SequencedFragment.class, FastqOutputFormat.class, sparkcontext.hadoopConfiguration());

    }*/

    public void writeRecords(JavaRDD<SAMRecord> records, Broadcast<SAMFileHeader> header, String outpath, SparkContext sc) {

        JavaPairRDD<SAMRecord, SAMRecordWritable> bamWritableRDD = readsToWritable(records, header);

        //Distribute records to HDFS as BAM

        bamWritableRDD.saveAsNewAPIHadoopFile(outpath, SAMRecord.class, SAMRecordWritable.class, BAMHeaderOutputFormat.class, sc.hadoopConfiguration());
    }


    public static JavaPairRDD<SAMRecord, SAMRecordWritable> readsToWritable(JavaRDD<SAMRecord> records, Broadcast<SAMFileHeader> header) {
        return records.mapToPair(read -> {

            //SEQUENCE DICTIONARY must be set here for the alignment because it's not given as header file
            //Set in alignment to sam map phase
            if(header.getValue().getSequenceDictionary()==null) header.getValue().setSequenceDictionary(new SAMSequenceDictionary());
            if(header.getValue().getSequenceDictionary().getSequence(read.getReferenceName())==null)
                header.getValue().getSequenceDictionary().addSequence(new SAMSequenceRecord(read.getReferenceName()));

            //read.setHeader(read.getHeader());
            read.setHeaderStrict(header.getValue());
            final SAMRecordWritable samRecordWritable = new SAMRecordWritable();
            samRecordWritable.set(read);
            return new Tuple2<>(read, samRecordWritable);
        });
    }

    public static JavaPairRDD<SAMRecord, SAMRecordWritable> readsToWritableNoRef(JavaRDD<SAMRecord> records) {
        return records.mapToPair(read -> {
            //read.setHeaderStrict(read.getHeader());
            read.setHeader(read.getHeader());
            final SAMRecordWritable samRecordWritable = new SAMRecordWritable();
            samRecordWritable.set(read);
            return new Tuple2<>(read, samRecordWritable);
        });
    }

    public static JavaPairRDD<SAMRecord, SAMRecordWritable> readsToWritableNoHeader(JavaRDD<SAMRecord> records) {
        return records.mapToPair(read -> {
            final SAMRecordWritable samRecordWritable = new SAMRecordWritable();
            samRecordWritable.set(read);
            return new Tuple2<>(read, samRecordWritable);
        });
    }

    public static JavaRDD<SAMRecord> setPartitionHeaders(final JavaRDD<SAMRecord> reads, final Broadcast<SAMFileHeader> header) {

        return reads.mapPartitions(records -> {
            //header.getValue().setTextHeader(header.getValue().getTextHeader()+"\\n@SQ\\tSN:"+records..getReferenceName());
            //record.setHeader(header);

            BAMHeaderOutputFormat.setHeader(header.getValue());
            return records;
        });
    }

    public static class BAMHeaderOutputFormat extends KeyIgnoringBAMOutputFormat<NullWritable>{
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
