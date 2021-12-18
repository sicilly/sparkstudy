package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    // 建立和spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // 执行业务操作
    // 读取一行行数据
    val lines: RDD[String] = sc.textFile("datas/*")
    // 分成一个个单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 每个单词计数为1
    val wordToOne = words.map(
      word=>(word,1)
    )
    // 相同单词放在一组（分组）
    val wordGroup: RDD[(String,Iterable[(String,Int)])] = wordToOne.groupBy(
      t => t._1
    )
    // 相同单词数量相加（聚合）
    val wordToCount = wordGroup.map{
      case(word,list)=>{
        list.reduce(
          (t1,t2)=>{
            (t1._1,t1._2+t2._2)
          })}}
    // 输出结果
    val array:Array[(String,Int)] = wordToCount.collect()
    array.foreach(println)

    // 关闭连接
    sc.stop()
  }
}
