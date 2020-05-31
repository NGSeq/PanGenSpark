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

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by root on 6/6/16.
  */
//Find heaviest path from matrix row block sums with constant length
//This assumes we can jump over k rows multiple times
object HPFloydWarshall {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("HPFloydWarshall")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val nseqs = args(1)
    val seqsize = args(2)
    //can not be greater than nseqs
    val kp = args(3).toInt

    val scoresRows = sc.wholeTextFiles(args(0)).map{f =>
            val text = f._2

      //TODO: what if we init Array[Score] array with bases, then we don't need to read sequence files again. However, matrix will be nseqsX3.3G for full genomes and score array should be parallelized also
            val scv = new Array[Double](seqsize.toInt)
            var ok = true

            val lines = text.split("\n")
      //TODO: add gaps from gap files, read files first to array and shift real position to the right depending on number of consecutive gaps before current position
            for(line <- lines){
                  val parts = line.split(" ")
                  val pos = parts(0).trim.toInt
                  val len = parts(1).trim.toInt
                  var j = pos
                  for (j <- pos to (pos + len)) {
                      scv(j) += 1
                  }

            }

            val split = f._1.split("/")
            val fastafile = split(split.length - 1)
            val regx = new Regex("\\d+")
            val seqno = (regx findAllIn fastafile).mkString("").toInt-1
            // fastafile.substring(0, fastafile.length - 3)
            //println(" seq:"+seqno)

            //add sequence to IndexedRow
            new IndexedRow(seqno, Vectors.dense(scv))
            //val v = Vectors.dense(scv)
            //val m = Matrices.ones(30,1)

    }

    for(row <- scoresRows.collect()) println("row:"+row.toString)

    val indScoreMat = new IndexedRowMatrix(scoresRows)

    println("indmatsize: cols "+indScoreMat.numCols()+" rows "+ indScoreMat.numRows())
    val ones =  new Array[Double](30)
    for (i<- 0 to ones.size-1) ones(i)=1

    //val indrow =  new IndexedRow(0, Vectors.dense(ones))
    //val indrow = new IndexedRowMatrix(sc.parallelize(ones))

/*
    val shortestPaths = Set(
      (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
      (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))
    val edgeSeq = Seq((1, 2), (1, 5), (2, 3), (2, 5), (3, 4), (4, 5), (4, 6)).flatMap {
      case e => Seq(e, e.swap)
    }

    val edges = sc.parallelize(edgeSeq).map { case (v1, v2) => (v1.toLong, v2.toLong) }
    val graph = Graph.fromEdgeTuples(edges, 1)
    val landmarks = Seq(1, 4).map(_.toLong)
    val results = ShortestPaths.run(graph, landmarks).vertices.collect.map {
      case (v, spMap) => (v, spMap.mapValues(i => i))
    }*/

    val scoreBlocks = indScoreMat.toBlockMatrix(20,30).blocks.map{b =>

      //val doubles = b._2.toArray
      //val max = doubles.reduceLeft(_ max _)
      //Not working here
      //for (i<- 0 to doubles.size-1) println("x:"+b._1._2+" y:"+b._1._1+" i:"+i+ "d:"+doubles(i)+" max:"+max)
        //b._2.transpose.multiply(Vectors.dense(ones))
      new Tuple2(b._1, b._2)
    }

    /*val bmfilt = indScoreMat.toBlockMatrix(20,30).blocks.filter{case ((x, y), matrix) => x == 0 && y == 0
    }
    val b = new BlockMatrix(bmfilt, 30, 20)
    */
    //for( d <- scoreSumBlocks.values) println(d)

    scoreBlocks.count()

    //TODO: try using class for storing scores and bases
    //Init coordinate map for storing start-end positions in sequence row

    scoreBlocks.foreach(smb =>  {

      //for (i<- 0 to f.values.size-1)
      //  println(f.values(i))
      //val den = f._2.multiply(Vectors.dense(ones))


      //val block = smb._2
      //For testing
      val block = Array(Array(1,2,3), Array(3,2,1), Array(4,5,6))
      //val doubles = smb._2.toArray
      //val max = doubles.reduceLeft(_ max _).toInt

      var refstartpos = 0
      var refendpos = 0
      var refseq = 0
      var found = false

      //val cols = smb._2.numCols
      //val rows = smb._2.numRows
      //val n = (cols)*(rows)

      //For testing
      val cols = block(0).size
      val rows = block.size
      val n = (cols)*(rows)

      /*val shortestPaths = Set(
        (1, Map(1 -> 0, 4 -> 2)), (2, Map(1 -> 1, 4 -> 2)), (3, Map(1 -> 2, 4 -> 1)),
        (4, Map(1 -> 2, 4 -> 0)), (5, Map(1 -> 1, 4 -> 1)), (6, Map(1 -> 3, 4 -> 1)))*/

      //TODO: fill this array, each vertex has edge to each k-nearest vertices in following column
      //for(k<-0 to 3)

      /*var edgelist = ListBuffer(Edge)
          for(i<-kp to smb._2.numCols-1-kp)
            for(j<-kp to smb._2.numRows-1-kp)
              for(k <- -kp to kp){

                edgelist.apply((i+1)*(j+k))
                edgelist :+ (Edge((j*i), (i+1)*(j+k), block.apply(j,i)))
              }*/

      /*All pairs shortest path
      for k = 1 to n do:
      for each i,j do:
      A[i][j] = min( A[i][j], (A[i][k] + A[k][j]);*/

      //TODO: fill adjacency matrix with score values by means of allowed rows (vertexes has edges to k-nearest values on next column) and the heaviest path is then calculated with Floyd's algorithm
      //TODO: use sparsematrix http://netlib.org/linalg/html_templates/node92.html

          /*for(k <- 0 to kp){
            for(i<-0 to smb._2.numCols-1-kp)
              for(j<-0 to smb._2.numRows-1-kp){
                val sum = (block.apply(i, (j+k))+block.apply((i+1), j))
                if(path.apply(j*i) < sum)
                  path.update(j*i,sum)
              }

          }*/

      //var adj = Matrices.sparse(n,n)
      //val iind = scala.collection.mutable.ArrayBuffer.empty[Int]

      var ri = 0
      var bi = 0
      val adjmatrix = ArrayBuffer.empty[Array[Double]]
      //var adjrows = ArrayBuffer.empty[Double]
      for(j<-0 to rows-1){

        //val adjrow = ArrayBuffer.empty[Int]
        for(i<-0 to cols-1){

          //TODO: change to Long
            val adjcolind = ArrayBuffer.empty[Int]
            val vals = ArrayBuffer.empty[Double]
            var g = 0
            for(k <- (-kp) to kp){
                //i*(j+k)

              var startblock = (bi-kp)
              if(startblock<0)
                startblock = 0
              //still something wrong
              adjcolind.+=(startblock*(cols) + i + g*(cols))
              vals.+=(block(j)(i))
              g+=1
            }
            adjmatrix.+=(Vectors.sparse(n, adjcolind.toArray, vals.toArray).toArray)

            ri += 1
          }
        bi += 1
      }

      //remove this part, use vector array
      //val adjmat = Matrices.dense(n,n,adjrows.toArray)

      //val colPtrs = Array(iind: _*)
      //val rowPtrs = Array(jind: _*)
      //TODO: use array of Long from the start, Matrix cannot be used because it's immutable, try 2dim arrays
      //val distances = ArrayBuffer.empty[Double]
      val edges = ArrayBuffer.empty[Array[Tuple3[Int, Int, Double]]]

      for(j<-0 to n-1){
        var edgerow = ArrayBuffer.empty[Tuple3[Int, Int, Double]]
        for(i<-0 to n-1){
          val score = adjmatrix(j)(i).toLong
          edgerow.+=(Tuple3(j,i,score))
          //distances.+=:(score)
        }
        edges.+=(edgerow.toArray)
      }


      println("_____________________ADJMAT INITIAL_________________________")
      for(j<-0 to n-1){
        println("")
        for(i<-0 to n-1)
        //val path = ArrayBuffer.empty[Tuple3[Int, Int, Long]]
          print("("+j+","+i+"):"+adjmatrix(j)(i)+" ")
      }

      println("_____________________EDGES INITIAL_________________________")
      for(j<-0 to n-1){
        println("")
        for(i<-0 to n-1)
            print("("+edges(j)(i)._1+","+edges(j)(i)._2+") ss:"+edges(j)(i)._3+" ")
      }


        for(j<-0 to n-1)
          for(i<-0 to n-2){
            for(k <- -kp to kp){
              var g = j+k
              if(g < 0)
                g = 0
              val sum = adjmatrix(j)(i)+adjmatrix(g)(i+1)
              if(adjmatrix(j)(i) < sum){
                //TODO: add to (i*k) to another struct and extract path
                //https://en.wikipedia.org/wiki/Floyd%E2%80%93Warshall_algorithm
                adjmatrix(j).update(i, sum)
                //what if update just when j-kp/2<k<j+kp/2
                edges(j).update(i, edges(g).apply(i+1))
                //val max = doubles.reduceLeft(_ max _)
                //println("i:"+i+" j:"+ j +" sum:"+sum)}
            }
          }
        }

      println("_____________________EDGES UPDATED_________________________")
      for(j<-0 to n-1)
        for(i<-0 to n-1)
          if(edges(j)(i)._3 != 0)
            println("src:"+edges(j)(i)._1+" dst:"+edges(j)(i)._2+" ss:"+edges(j)(i)._3+" / ")

      //see http://algs4.cs.princeton.edu/44sp/FloydWarshall.java.html
      println("_____________________NON NULL DISTANCES_________________________")
      //We want paths from the start of rows to the end of rows
      //test just first row

        for(j<-0 to n-1)
          for(i<-0 to n-1)
            if(adjmatrix(j)(i)!=0){
              //val path = ArrayBuffer.empty[Tuple3[Int, Int, Long]]
              println("dist j,i:"+j+","+i+" sum:"+adjmatrix(j)(i)+"/")
            }

      println("_____________________ADJMAT FINAL_________________________")
      for(j<-0 to n-1){
       println("")
        for(i<-0 to n-1)
            //val path = ArrayBuffer.empty[Tuple3[Int, Int, Long]]
            print("("+j+","+i+"):"+adjmatrix(j)(i)+" ")
      }

      //Extract Heaviest path
     /* for(i <- 0 to n-1){
        var max: Long = 0
        var col = 0
        var row = 0
        for(j <- 0 to n-1){
          val v = adj.apply(i*j+600)
          if(v > max){
            max = v
            row = j
            col = i
          }
        }
        print(col+","+ row +","+max+"/")

      }*/
     /* //for matrix operations
     for(k <- 0 to n-1){
        for(i<-0 to n-1)
          for(j<-0 to n-1){
            val sum = adj.apply(k, j)+adj.apply(i, k)

            if(adj.apply(i,j) < sum){
              println(adj.apply(i,j))
              //TODO: store this to other datastructure that column-wise max can be easily extracted
              adj(i,j) == sum
              //val max = doubles.reduceLeft(_ max _)
              println("i:"+i+" j:"+ j +" sum:"+sum)
            }

          }

      }*/

      println("_______________________________________________________")
      //println(adj.numNonzeros)
    })
    //TODO: combine blocks to create final sequence
    //val vectsums = indScoreMat.multiply(Matrices.ones(30,1))
  }

}
