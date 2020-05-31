package org.ngseq.pangenomics.tools;

import htsjdk.samtools.seekablestream.SeekableFileStream;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFHeader;
import org.apache.commons.collections4.iterators.IteratorIterable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.seqdoop.hadoop_bam.KeyIgnoringVCFOutputFormat;
import org.seqdoop.hadoop_bam.VCFFormat;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import org.seqdoop.hadoop_bam.util.VCFHeaderReader;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Altti Ilari Maarala on 7/20/16.
 */
public class VCFPartitioner {
    //private static VCFHeader header;

    //Executing with Spark locally
    //spark-submit --master local[4] --class SparkHadoopBAM target/seqspark-1.6.1-jar-with-dependencies.jar /user/hdfs/test/input.bam /user/hdfs/test/out

    public static <U> void main(String[] args) throws IOException {

        SparkConf sparkconf = new SparkConf().setAppName("VCFPartitioner");
        JavaSparkContext sc = new JavaSparkContext(sparkconf);
        SQLContext sqlContext = new SQLContext(sc);

        String inputfile = args[0];
        String outpath = args[1];
        String headerfile = args[2];
        String geneIDfilter = args[3];

        JavaPairRDD<LongWritable, VariantContextWritable> vcwfRDD = sc.newAPIHadoopFile(inputfile, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class, sc.hadoopConfiguration());

        JavaRDD<VariantContext> vcfRDD = vcwfRDD.map(v -> v._2.get());
        //TODO: apply different filters
        if(geneIDfilter!=null)
            vcfRDD.filter(v -> {
                String id = v.getID();
                return id.equalsIgnoreCase(geneIDfilter);
            });

        //FileSystem fs = FileSystem.get(new Configuration());
        //TODO: parse headers automatically from vcf files
        VCFHeader vcfheader = VCFHeaderReader.readHeaderFrom(new SeekableFileStream(new File(headerfile)));
        final Broadcast<VCFHeader> headerBc = sc.broadcast(vcfheader);
        VCFHeaderOutputFormat.setVCFHeader(headerBc.getValue());

        JavaRDD<VariantContext> vcfh = vcfRDD.mapPartitions(vcfs -> {
            VCFHeaderOutputFormat.setVCFHeader(headerBc.getValue());
            //vcf._2.get().fullyDecode(headerBc.getValue(),false);
            return vcfs;
        });

        JavaPairRDD<VariantContext, VariantContextWritable> vrdd = vcfh.mapToPair(vcf -> {
            //VCFHeaderOutputFormat.setVCFHeader(headerBc.getValue());
            //vcf._2.get().fullyDecode(headerBc.getValue(),false);
            VariantContextWritable vcw = new VariantContextWritable();
            vcw.set(vcf);
            return new Tuple2(vcf, vcw);
        });

        //TODO: apply genotypes from VCF to corresponding positions in reference genome to create consensuses
        //Can be done in one iteration but this might lead to memory issues with standalone machine..
        /*JavaPairRDD<VariantContext, VariantContextWritable> vrdd = vcwfRDD.mapPartitionsToPair(vcfs -> {
            VCFHeaderOutputFormat.setVCFHeader(headerBc.getValue());
            List<Tuple2<VariantContext,VariantContextWritable>> vclist = new ArrayList<>();
            while(vcfs.hasNext()){
                VariantContextWritable vcw = vcfs.next()._2;
                vclist.add(new Tuple2<>(vcw.get(), vcw));
            }

            //vcf._2.get().fullyDecode(headerBc.getValue(),false);
            return vclist;
        });*/
        //vrdd.zipWithIndex();
        //JavaPairRDD<VariantContext, Iterable<VariantContextWritable>> groupped = vrdd.groupByKey();

        vrdd.saveAsNewAPIHadoopFile(outpath, VariantContext.class, VariantContextWritable.class, VCFHeaderOutputFormat.class, sc.hadoopConfiguration());
        //vcfRDD.filter(vc -> Boolean.valueOf(vc.getID()!= null));
        //vcfRDD.filter(vc -> Boolean.valueOf(vc.getID()!= null));

        //Create dataframes and query
        /*DataFrame dfreads = sqlContext.createDataFrame(vcfRDD, MyVariant.class);
        dfreads.registerTempTable("myvariants");
        //dfreads.groupBy("readgroup");
        DataFrame result = sqlContext.sql("SELECT bases, alleles, reference FROM myvariants");
        }*/


    }


    public static JavaPairRDD<VariantContext, VariantContextWritable> vcfToWritableNoRef(JavaRDD<VariantContext> records,  Broadcast<VCFHeader> header) {
        return records.mapToPair(vcf -> {
            //read.setHeaderStrict(read.getHeader());
            vcf.fullyDecode(header.getValue(),false);
            final VariantContextWritable recordWritable = new VariantContextWritable();
            recordWritable.set(vcf);
            return new Tuple2<>(vcf, recordWritable);
        });
    }

    public static class MyVariant implements Serializable {

        private String bases;
        //private String genotype;
        private String alleles;
        private String reference;


        public String getBases() {
            return bases;
        }

        public void setBases(String bases) {
            this.bases = bases;
        }

        public String getAlleles() {
            return alleles;
        }

        public void setAlleles(String alleles) {
            this.alleles = alleles;
        }

        public String getReference() {
            return reference;
        }
        public void setReference(String reference) {
            this.reference = reference;
        }

        /*public void setGenotype(String genotype) {
            this.genotype = genotype;
        }
        public String getGenotype() {
            return genotype;
        }*/
    }

    public static class MyVariantAdapter implements Serializable {

        private static final long serialVersionUID = 1L;
        private final VariantContext variantContext;

        public MyVariantAdapter(VariantContext vc) {
            this.variantContext = vc;
        }

        public static MyVariant adapt(VariantContext vcf) {
            MyVariant myvariant = new MyVariant();
            myvariant.setBases(vcf.getReference().getBaseString());
            myvariant.setAlleles(vcf.getAlleles().get(0).getBaseString());
            myvariant.setReference(vcf.getReference().getBaseString());
            return myvariant;
        }

        public int hashCode() {
            return this.variantContext.hashCode();
        }

    }
    public static class VCFHeaderOutputFormat extends KeyIgnoringVCFOutputFormat<NullWritable> {
        public static VCFHeader vcfheader;

        public VCFHeaderOutputFormat() {
            super(VCFFormat.VCF);
        }

        public static void setVCFHeader(VCFHeader VCFHeader) {
            vcfheader = VCFHeader;
        }

        /*public void setHeader(final VCFHeader header) {
            vcfheader = header;
        }*/

        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(TaskAttemptContext ctx,
                                                                             Path outputPath) throws IOException {
            // the writers require a header in order to create a codec, even if
            // the header isn't being written out
            setHeader(vcfheader);
            return super.getRecordWriter(ctx, outputPath);
        }
    }
    public static class VCFHeaderlessOutputFormat extends KeyIgnoringVCFOutputFormat<NullWritable> {


        public VCFHeaderlessOutputFormat() {
            super(VCFFormat.VCF);
        }

        @Override
        public RecordWriter<NullWritable, VariantContextWritable> getRecordWriter(TaskAttemptContext ctx,
                                                                                  Path outputPath) throws IOException {
            // the writers require a header in order to create a codec, even if
            // the header isn't being written out
            ctx.getConfiguration().setBoolean(WRITE_HEADER_PROPERTY, false);
            return super.getRecordWriter(ctx, outputPath);
        }
    }


}
