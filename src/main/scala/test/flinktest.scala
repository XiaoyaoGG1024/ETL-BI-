package test

import org.apache.flink.streaming.api.scala._

/**
 * @program: BigData
 * @description: ${description}
 * @author: 逍遥哥哥每天都要努力啊
 * @create: 2023/5/6
 * */
object flinktest {

    def main(args: Array[String]): Unit = {
      //1.创建一个执行环境(流处理)
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      //2.读取文本文件
      var lineDataStream: DataStream[String] = env.readTextFile("datas/wordCount.txt")

      //3.对数据集进行抓换处理
      var wordAndOne = lineDataStream.flatMap(_.split(" ")).map(word => (word, 1))


      //4.按照单词进行分组
      val wordAndOneGroup = wordAndOne.keyBy(_._1)


      //5.对分组数据进行sum聚合统计
      val sum = wordAndOneGroup.sum(1)


      //6.输出
      sum.print()


      //执行任务
      env.execute()

    }

  }



