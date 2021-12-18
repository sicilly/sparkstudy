package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    // 建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 执行业务操作
    // 读取一行行数据
    val lines: RDD[String] = sc.textFile("datas/*")
    // val lines: RDD[String] = sc.textFile("oss://spark-on-k8s-1/hp1.txt")
    // 分成一个个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 每个单词计数为1
    val wordToOne = words.map(
      word=>(word,1)
    )
    // 相同的key,对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey((x,y)=>{x+y})

    // 输出结果
    val array:Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
