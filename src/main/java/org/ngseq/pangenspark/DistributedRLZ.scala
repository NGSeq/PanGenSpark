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
import java.net.URI
import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scala.sys.process.Process

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object DistributedRLZ {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("DRLZ").getOrCreate()
    import spark.implicits._

    val localradix = args(0)
    val localref = args(1)

    val dataPath = args(2)
    val hdfsurl = args(3)
    val hdfsout = args(4)
    val hdfsgapless = args(5)

    val radixSA = "./radixSA"

    val localOut = "radixout.txt"
    val refParse = "./rlz_for_hybrid"
    val output = "merged.lz"

    println("Create suffix")
    val createSA =  Process(radixSA + " " + localref + " " + localradix).!

    println("Load suffix")
    val suffix = scala.io.Source.fromFile(localradix).getLines.toArray.map(_.toInt)

    println("broadcasting")
    val SA = spark.sparkContext.broadcast(suffix)

    // broadcast plain ref (needed for pattern matching)
    val ref = scala.io.Source.fromFile(localref).getLines.mkString("")

    val reference = spark.sparkContext.broadcast(ref)


    val gaps = spark.read.text(dataPath)
      .select(org.apache.spark.sql.functions.input_file_name, $"value")
      .as[(String, String)]
      .rdd.map{v=>
      //val groups = v.grouped(x._1._2.length()/numSplits).toArray
      //groups.zipWithIndex.map(y => (fileName,y._2,x._2,y._1))
      val gapless = v._2.replaceAll("-", "")

        val fname = v._1.toString.split("/")
        val header = ">"+fname(fname.length-1)+System.lineSeparator()

        header+gapless
    }

    gaps.saveAsTextFile(hdfsgapless)

    val splitted = spark.read.text(dataPath)
      .select(org.apache.spark.sql.functions.input_file_name, $"value")
      .as[(String, String)]
      .rdd.map{v=>
      val gapless = v._2.replaceAll("-", "")

      //val groups = v.grouped(x._1._2.length()/numSplits).toArray
      //groups.zipWithIndex.map(y => (fileName,y._2,x._2,y._1))
      (v._1,gapless.length,gapless)
    }

    println("removing")
    //val removed = new File(localOut).delete()

    // load the output
    println("loading to spark")


    // binary search that can find the upper and lower bounds
    // e.g for string 1111222555555666677 would return
    // (7,12) if we were trying to find 5
    // finds the interval for longest match
    // if i = 0 match length is 1. By increasing i
    // the ith positions are compared
    // essentially to find the longest match this function needs to called in loop
    // until the interval does not decrease
    def binarySearch(lb: Int, rb: Int, d_b: Broadcast[String], cur: Char, i: Int, SA_b: Broadcast[Array[Int]]): Option[(Int, Int)] = {
      val d = d_b.value
      var low = lb
      var high = rb
      while (low < high) {
        val mid = low + ((high - low) / 2)
        // get the true position
        val midKey = SA_b.value(mid) + i

        // different "layers"
        val midValue = if (midKey < d.length()) {
          d(midKey)
        } else {
          '1'
        }
        //println("lb: " + low + " rb: " + high + " mid: " + mid + " key: " + midValue)

        if (cur <= midValue) {
          high = mid
        } else {
          low = mid + 1
        }
      }
      val low_res = low
      //println("low: " + low)
      //println("----------------")

      // break if key not found
      if ((SA_b.value(low_res) + i)>= d.length || d(SA_b.value(low_res) + i) != cur) {
        return None
      }
      high = rb
      while (low < high) {
        val mid = low + ((high - low) / 2)
        val midKey = SA_b.value(mid) + i
        // different "layers"
        val midValue = if (midKey < d.length()) {
          d(midKey)
        } else {
          '1'
        }
        //println("lb: " + low + " rb: " + high + " mid: " + mid + " key: " + midValue)
        if (cur >= midValue) {
          low = mid + 1
        } else {
          high = mid
        }
      }
      //println("value: " + d(SA.value(low) + i) + " cur: " + cur + " lo: " + low)
      if (SA_b.value(low) != d.length() - 1 && SA_b.value(low)+i< d.length() && d(SA_b.value(low) + i) != cur) {
        //if(low_res>low-1) {
        //  return Some((low_res,low))
        //}
        return Some((low_res, low - 1))
      }
      Some((low_res, low))
    }

    // lb rb clear
    // start j-i
    // cur substring[j]
    // d ref
    //def refine(lb: Int, rb: Int, start: Int, cur: Char, d: String): (Int, Int) = {
    //  val (lb_new,rb_new) = binarySearch(d.slice(SA.value(start,d.length())),cur)
    //}

    // check newline to deal with partition borders (stop phrase search if goes
    // to newline
    def factor(i: Int, x: String, d_b: Broadcast[String], SA_b: Broadcast[Array[Int]]): (String, Long) = {
      val d = d_b.value
      var lb = 0
      var rb = d.length()-1 // check suffix array size
      var j = i
      breakable {
        while (j < x.length()) {
          //println("j: " + j + " SA.value: " + SA.value(lb))
          //println((SA.value(lb)+j-i) + " " + d.length())

          if (lb == rb && d(SA_b.value(lb) + j - i) != x(j)) {
            break
          }
          //(lb,rb) = refine(lb,rb,j-i,x(j))
          val tmp = binarySearch(lb, rb, d_b, x(j), j - i,SA_b)
          //println(tmp)

          if (tmp == None) {
            break
          } else {
            //println("jassoo")
            val tmp_tuple = tmp.get
            lb = tmp_tuple._1
            rb = tmp_tuple._2

          }
          j += 1
          // border
          if (j == x.length()) {
            break
          }
        }
      }
      //println("out")
      if (j == i) {
        return (x(j).toString(), 0)
      } else {
        //println("täällä")

        return (SA_b.value(lb).toString(), j - i)
      }
    }

    // encode a single substring x
    // finds the longest possible match and returns
    // (pos,len) pair(s)
    def encode(x: String, d_b: Broadcast[String],SA_b: Broadcast[Array[Int]]): ArrayBuffer[(String, Long)] = {
      var i: Int = 0
      val max = Int.MaxValue
      val output = ArrayBuffer[(String, Long)]()
      while (i < x.length()) {
        //println(i)
        val tup = factor(i, x, d_b,SA_b)
        //println("<<<<<<<\n"+tup+"\n<<<<<<<")
        output += tup
        if (tup._2 == 0) {
          i += 1
        } else {
          if(i+tup._2>=max) {
            i = x.length()
          } else {
            i += tup._2.toInt
          }
        }
      }
      return output
    }
    println("started encoding")
    //val rsize = (ref._2.length()).toString

    //val maxSplit = filteredTmp.map(_._2).max()
    splitted.foreach{x =>
      val encodings = encode(x._3,reference,SA)

      var fos: FSDataOutputStream = null
      val fis = FileSystem.get(new URI(hdfsurl),new Configuration())

      try {
        //val nf = new DecimalFormat("#0000000")
        val fname = x._1.toString.split("/")

        fos = fis.create(new Path(hdfsout+"/" + fname(fname.length-1)+".lz"))
      } catch {
        case e: IOException =>
          //e.printStackTrace()
      }

      encodings.foreach{z =>
        //println(x._1+","+x._2)
        var posBytes: Array[Byte] = null

        val len = z._2
        if(len != 0) {
          posBytes = ByteBuffer.allocate(8).putLong(z._1.toLong).array.reverse
          //posBytes = x._1.getBytes
        }
        else {
          //posBytes = ByteBuffer.allocate(8).putLong(rsize).array.reverse
          //len = 1
          posBytes = ByteBuffer.allocate(8).putLong(z._1(0).toLong).array.reverse
          //posBytes = x._1(0).toString.getBytes
        }
        val lenBytes = ByteBuffer.allocate(8).putLong(len).array.reverse
        //(posBytes,lenBytes)
        //val lenBytes = len.toString.getBytes

        //println(x._1,len)
        fos.write(posBytes)
        fos.write(lenBytes)

      }

      fos.close()

    }

    spark.stop()

  }
}
