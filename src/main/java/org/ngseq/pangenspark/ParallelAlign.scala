/*
  * Copyright 2020 Altti Maarala
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *       http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
*/
package org.ngseq.pangenspark

import java.io._

import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.sys.process._

object ParallelAlign {

 def main(args: Array[String]) {
  
    val spark = SparkSession.builder.appName("ParallelAlign").getOrCreate()
    import spark.implicits._

    val indexPathHDFS = args(0)
    val indexPath = args(1)
    val outputPath = args(2)
    val splits = args(3).toInt
    val paired = args(4).toInt
    val threads = args(5).toInt
    val readsPath1 = args(6)
    val readsPath2 = args(7)
    val sampath = args(8)
    val hdfsP = "/user/root/"+sampath

    val prefix = "/tmp/out/reads"
    var samFiles: Array[String] = Array()
    val aligner_bin = "/opt/bowtie2/bowtie2"

    def readPaired(path: String): RDD[(String,String)] = {
      return spark.sparkContext.textFile(path)
      .sliding(4,4).map(x => (x(0).dropRight(2),x.mkString("\n")))
    }

    // call bowtie parallel
    // repartition value should be experiemented
    // if paired reads (we need to match each pair)
    if(paired==1) {
      val read1 = readPaired(readsPath1).toDS
      val read2 = readPaired(readsPath2).toDS

      // join by key
      val reads = read1.join(read2,"_1").rdd


      val out = reads.repartition(splits).glom().zipWithIndex.map{x=>

          // save locally read files
          val createFolder = Process("mkdir -p " + prefix).!

          // save parts of read 1 locally
          val id = System.nanoTime()

          val localPath1 = prefix+"/read."+x._2.toInt+".1.fq"
          val localPath2 = prefix+"/read."+x._2.toInt+".2.fq"
          // add "id" to read.fq name some that multiple reads do not get
          // mixed up
          val pw = new PrintWriter(new File(localPath1))
          x._1.foreach{y=>
            pw.write(y.getString(1)+"\n")
          }
          pw.close()
          val pw2 = new PrintWriter(new File(localPath2))
          x._1.foreach{y=>
            pw2.write(y.getString(2)+"\n")
          }
          pw2.close()
          // collect index if needed
          val createIndexFolder = Process("mkdir -p " + indexPath).!
          val indexP = new File(indexPath)
          if(indexP.isDirectory() && indexP.listFiles().isEmpty) {
            val getIndex = Process("hdfs dfs -get " + indexPathHDFS + " " + indexPath + "/").!
          }
          val sam = prefix + "/" + id + x._2.toInt + ".sam"
          val indexName = indexP.listFiles().sorted.head.toString.split("\\.").head + ".fa.P512_GC4_kernel_text"
          val command = aligner_bin + " --no-unal -p " + threads + " -x " + indexName + " -S " +
            sam  +
            " -1 " + localPath1 + " -2 " + localPath2

            val out = new StringBuilder
            val err = new StringBuilder

          //val logger = ProcessLogger(
           // (o: String) => out.append(o),
            //(e: String) => err.append(e))
        println("CMD:!!!!"+command)
          val res = Process(command).!
        //(logger)
          val pw5 = new PrintWriter(new File(prefix+"/d.log"))
          pw5.write(command+"\n")
          pw5.write(out.mkString("")+"\n")
          pw5.write(err.mkString(""))
          pw5.close()

          val out2 = new StringBuilder
          val err2 = new StringBuilder
          val logger2 = ProcessLogger(
            (o: String) => out2.append(o),
            (e: String) => err2.append(e))

          val outputPath = hdfsP+id + x._2.toInt + ".sam"
          val addCommand = "hdfs dfs -put " + sam + " " + hdfsP
          val addHDFS = Process(addCommand).!(logger2)
          val pw6 = new PrintWriter(new File(prefix+"/e.log"))
          pw6.write(addCommand+"\n")
          pw6.write(out2.mkString("")+"\n")
          pw6.write(err2.mkString(""))
          pw6.close()

          val removeLocal = new File(prefix).listFiles().filter{z => 
            z.isFile && 
            (z.toString.contains("read."+x._2.toInt+".") ||
             z.toString.contains(sam))}.foreach(_.delete())
          outputPath
      }
      samFiles = out.collect()
    } else {
      val reads = spark.sparkContext.textFile(readsPath1)
        .sliding(4,4).map(x => x.mkString("\n"))


      val out = reads.repartition(splits).glom().zipWithIndex.map{x=>

          // save locally read files
          val createFolder = Process("mkdir -p " + prefix).!

          // save parts of read 1 locally
          val id = System.nanoTime()

          val localPath1 = prefix+"/read."+x._2.toInt+".1.fq"
          // add "id" to read.fq name some that multiple reads do not get
          // mixed up
          val pw = new PrintWriter(new File(localPath1))
          x._1.foreach{y=>
            pw.write(y+"\n")
          }
          // collect index if needed
          val createIndexFolder = Process("mkdir -p " + indexPath).!
          val indexP = new File(indexPath)
          if(indexP.isDirectory() && indexP.listFiles().isEmpty) {
            val getIndex = Process("hdfs dfs -get " + indexPathHDFS + " " + indexPath+"/").!
          }
          // not paired
          val sam = prefix + "/" + id + x._2.toInt + ".sam"
          val indexName = indexP.listFiles().sorted.head.toString.split("\\.").head + ".fa.P512_GC4_kernel_text"
          val command = aligner_bin + " --no-unal -p " + threads + " -x " + indexName + " -S " +
            sam  + " -U " + localPath1
          val res = command !

          val outputPath = hdfsP+id + x._2.toInt + ".sam"
          val addHDFS = Process("hdfs dfs -put " + sam + " " + hdfsP).!

          val removeLocal = new File(prefix).listFiles().filter{z => 
            z.isFile && 
            (z.toString.contains("read."+x._2.toInt+".") ||
             z.toString.contains(sam))}.foreach(_.delete())
          outputPath
      }
      samFiles = out.collect()
    }

    // calculate rest of the hybrid index one a single machine

    // get sam files from hdfs and concatenate

    val samFilesConcat = samFiles.mkString(" ")
    val get = Process("hdfs dfs -get " + samFilesConcat + " "+sampath).!
    // concatenate
    val concatenate = Process("hdfs dfs -getmerge " + hdfsP +" "+ outputPath + "/out.sam").!

  } 
}