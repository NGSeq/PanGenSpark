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

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable.ArrayBuffer

object ScoreMatrix {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("ScoreMatrix").getOrCreate()

    // position files in hdfs
    val posFolder = args(0)
    // pan genome (has gaps) files in hdfs 
    val panFolder = args(1)
    // genome length
    val msaLength = args(2).toInt
    // number of blocks for block matrix
    val numBlocks = args(3).toInt
    // number of patients (controls partitioning)
    val count = args(4).toInt
    // depth of treeReduce (should increase when the number of patients increases)
    // for 15G of pan genome 2 is enough
    val depth = args(5).toInt
    val outpath = args(6)
    val hdfsurl = args(7)
    //construct matrix from pos files

    //load position files and their names
    val positions = spark.sparkContext.wholeTextFiles(posFolder,count)
    .map(x => (x._1.split("/").last.split('.').head,x._2)).zipWithIndex.map(x => (x._2,x._1._2))

    // load panData
    val panData = spark.sparkContext.wholeTextFiles(panFolder,count).map(x => (x._1.split("/").last.split('.').head,x._2)).zipWithIndex.map(x => (x._2,x._1._2))

    // join postition and whole genome data
    val joined = positions.join(panData)

    /**
    * consecutive finds the length of consecutive sequence in gap starting at start
    */
    def consecutive(start: Int, gap: Array[Int]): Int = {
      var next = start+1
      var len = 1
      var i = start
      while(next<gap.length) {
        if(gap(i)+1!=gap(next)) {
          return len
        } else {
          len += 1
          next += 1
          i += 1
        }
      }
      return len
    }
    // create indexRows in sparse representation
    // this implementation first calculates a helper array that has the
    // correct positions (gap information taken into account)
    val processing = joined.map{x =>

      if(x._2._1.isEmpty()) {
        new IndexedRow(x._1,new SparseVector(msaLength,Array[Int](),Array[Double]()))
      } else {
        // find gaps
        val res = for {
          i <- 0 until x._2._2.length() if(x._2._2(i)=='-')
        } yield i
        var output = Array.fill[Double](msaLength)(0.0)
        val row = x._1
        val gap = res.toArray
        // calculate mapping from real positions based on gap information
        val mapping = Array.fill[Int](msaLength-gap.length)(0)

        // populate the hepler array
        var i = 0
        var mIndex = 0
        while(i < mapping.length) {
          if(mIndex<gap.length) {
            while(i<mapping.length && i < gap(mIndex) && mIndex+i != gap(mIndex)) {
              mapping(i) = mIndex+i
              i += 1
            }
            val cLen = consecutive(mIndex,gap)
            var j = 0
            var stop = false
            while(j<cLen && !stop && (i+j)<mapping.length) {
              mapping(i+j) = j+cLen+mIndex+i
              j += 1
              
              if(mIndex != 0 && mIndex+cLen<gap.length && gap(mIndex+cLen)<=(j+cLen+mIndex+i)) {
                stop = true
              }
            }
            mIndex += cLen
            i += j
          } else {
            // when there are no gaps left process rest
            while(i<mapping.length) {
              mapping(i) = mIndex+i
              i += 1
            }
          }
        }
        // use mapping information to change the pos coordinates
        // to coordatnes that has gap information included
        x._2._1.split("\n").par.foreach {i=>
          val pair = i.split(" ")
          val pos = pair.head.toInt
          val matchLength = pair.last.toInt

          for(i <- 0 until matchLength) {
            try {
              val mpos = mapping(pos+i)
              output(mpos) += 1
            }catch{
              case e: ArrayIndexOutOfBoundsException  =>
                println("MAPPING LEN:"+mapping.length)
                println("MAPPING pos:"+(pos+i))
                println("OUTPUT LEN:"+output.length)
                e.printStackTrace()
            }

          }
        }
        // change to sparse representation
        val indices = ArrayBuffer[Int]()
        val values = ArrayBuffer[Double]()
        for(i <- 0 until msaLength) {
          val value = output(i)
          if(value!=0) {
            indices += i
            values += value
          }
        }
        new IndexedRow(row,new SparseVector(msaLength,indices.toArray,values.toArray))
      }
    }




    // Create the required matrices and transpose the result.
    val coordMat: IndexedRowMatrix = new IndexedRowMatrix(processing)
    val blocks: BlockMatrix = coordMat.toBlockMatrix(coordMat.numRows.toInt,numBlocks).transpose


    println("numCols: " + blocks.numCols() + " numRows: " + blocks.numRows())

    // find the heaviest path scores
    val indexed = blocks.toIndexedRowMatrix().rows

    val path = indexed.map(x => (x.index,x.vector.argmax))
    .sortBy(_._1).collect()
    
    // broadcast the heaviest path
    val localB = spark.sparkContext.broadcast(path)

    // OR for each genome to find the heaviest path
    val mapped = panData.map{x => 
      val s = localB.value.size
      var j = 0
      for{
        i <- 0 until msaLength
      } yield if(j<s && localB.value(j)._1==i && localB.value(j)._2==x._1) {
          j += 1
          x._2(localB.value(j-1)._1.toInt)
        } else if(j<s && localB.value(j)._1==i){
          j += 1
          '?'
        } else {
          x._2(i)
        }  
    }

    // combine the results
    // treeReduce has to be used, because the datasizes are still large at this point
    val reduce = (a: IndexedSeq[Char],b: IndexedSeq[Char]) => {
      for(i <- 0 until a.size) yield if(a(i)=='?') b(i) else a(i)
    }

    val combine = mapped.treeReduce(reduce,depth).mkString("")

    // write the created adhoc reference to local filesystem
    println("LENGTH OF ADHOC!!!!!: "+combine.length)
    //val pw = new PrintWriter(new File("adhoc_ref"))
    //pw.write(combine.mkString(""))
    //pw.close()
    var fos: FSDataOutputStream = null
    val fis = FileSystem.get(new URI(hdfsurl),new Configuration())

    try {

      fos = fis.create(new Path(outpath))
      fos.writeBytes(combine);
    } catch {
      case e: IOException =>
      //e.printStackTrace()
    }
    fos.close()
    /*mapped.foreach(seq => {
      var fos: FSDataOutputStream = null
      val fis = FileSystem.get(new URI(hdfsurl),new Configuration())

      try {

        fos = fis.create(new Path(outpath+"_"+seq.length()+"_"+UUID.randomUUID().toString))
        fos.writeBytes(seq.toString());
      } catch {
        case e: IOException =>
        //e.printStackTrace()
      }
      fos.close()

    })*/

    spark.stop()
  }


}