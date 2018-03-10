package main.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.spark_project.guava.collect.Maps

object Main {
  def main(args: Array[String]): Unit = {

    println("여기서부터시작")

    Maps.newHashMap()

    val conf = new SparkConf()

    /**
      * The master URL to connect to, such as "local" to run locally with one thread, "local[4]" to
      * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
      */

    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc  = new SparkContext(conf)

//    val textFile = sc.textFile("hdfs:///test/shakespeare.txt")
    val textFile = sc.textFile("src/main/resources/shakespeare.txt")

    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words : " + counts.count())
//    counts.saveAsTextFile("hdfs:///test/shakespeareWordCount")
    counts.saveAsTextFile("/tmp/result")

  }
}
