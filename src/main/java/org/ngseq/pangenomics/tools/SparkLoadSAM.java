package org.ngseq.pangenomics.tools;

import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.SAMRecord;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.KeyIgnoringBAMOutputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by Altti Ilari Maarala on 7/20/16.
 */
public class SparkLoadSAM {

    private JavaSparkContext sparkcontext;

    public JavaPairRDD<LongWritable, SAMRecordWritable> getRecords() {
        return records;
    }

    private final JavaPairRDD<LongWritable, SAMRecordWritable> records;

    public SparkLoadSAM(JavaSparkContext sc) throws IOException {

        this.sparkcontext = sc;
        records = null;
    }

    public SparkLoadSAM(JavaSparkContext sc, String inputpath, boolean broadcastHeader) throws IOException {

        SQLContext sqlContext = new SQLContext(sc);

        String inputfile = inputpath;

        //If we want to distribute BAM file to HDFS, we read header from original BAM file and broadcast it

        if(broadcastHeader){
            SAMFileHeader header = SAMHeaderReader.readSAMHeaderFrom(new Path(inputfile), sc.hadoopConfiguration());
            final Broadcast<SAMFileHeader> headerBc = sc.broadcast(header);
        }

        records = sc.newAPIHadoopFile(inputfile, AnySAMInputFormat.class, LongWritable.class, SAMRecordWritable.class, sc.hadoopConfiguration());

        //print results
        /*for (Row r: result.collect()) {
            System.out.println("readgroup: "+ r.getString(0));
            System.out.println("start: " +r.getInt(1));
            System.out.println("length: " +r.getInt(2));
            System.out.println("bases: " +r.getString(3));
            System.out.println("cigar: " +r.getString(4));
        }*/

    }


    public static JavaPairRDD<SAMRecord, SAMRecordWritable> readsToWritable(JavaRDD<SAMRecord> records, Broadcast<SAMFileHeader> header) {
        return records.mapToPair(read -> {
            read.setHeaderStrict(header.getValue());
            final SAMRecordWritable samRecordWritable = new SAMRecordWritable();
            samRecordWritable.set(read);
            return new Tuple2<>(read, samRecordWritable);
        });
    }

    public static JavaPairRDD<SAMRecord, SAMRecordWritable> readsToWritable(JavaRDD<SAMRecord> records) {
        return records.mapToPair(read -> {
            read.setHeaderStrict(read.getHeader());
            final SAMRecordWritable samRecordWritable = new SAMRecordWritable();
            samRecordWritable.set(read);
            return new Tuple2<>(read, samRecordWritable);
        });
    }



}
