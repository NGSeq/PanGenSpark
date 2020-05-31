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

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object GroupSams {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("GroupSams").getOrCreate()

    val alignments = args(0)
    val out = args(1)

    val sams = spark.read.option("sep","\t").option("inferSchema", "true").csv(alignments)

    //val tmp1 = samc1.withColumn("_c0", regexp_replace(col("_c0"), "k", ">k"))
    //tmp1.select(concat(col("_c0"), lit("_"), col("_c2")) as "id", col("_c9"))

    sams.withColumn("seq", col("_c2")).coalesce(1).write.mode("overwrite").partitionBy("seq").option("header", "false").option("sep","\t").mode(SaveMode.Append).csv(out)

    spark.stop()

  }
}
