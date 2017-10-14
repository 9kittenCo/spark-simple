package exercises

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exercise1 {

  lazy val ss: SparkSession = SparkSession.builder().appName("exercise1").config("spark.master", "local").getOrCreate

  // This import is needed to use the $-notation
  import ss.implicits._

  // 1.
  def main(args: Array[String]): Unit = {

    val userRdd: RDD[(Int, String)] = readAndFilterUser()


    val carDF: DataFrame = readAndFilterCar()

    val userDF: DataFrame = userRdd.toDF("user_id", "name")

    val result: DataFrame = userDF.join(carDF, "user_id")

    val parqetPath = "src/main/resources/exercises/answers/part1.parquet"
    val csvRawPath = "src/main/resources/exercises/answers/rawCsv1.csv"

    FileUtil.fullyDelete(new File(parqetPath))
    result.write.parquet(parqetPath)

    saveCSV(result, csvRawPath, "part1.csv")

    ss.stop
  }

  //** @return RDD with users with valid == 1 */
  def readAndFilterUser(): RDD[(Int, String)] = {
    val usersRdd: RDD[String] = ss.sparkContext.textFile("src/main/resources/exercises/user.txt")

    val splittedRdd: RDD[Array[String]] = usersRdd map (_.split(","))

    splittedRdd filter (_ (2) == "1") map (ar => (ar(0).toInt, ar(1)))
  }

  //** @return DataFrame with cars with valid == 1 */
  def readAndFilterCar(): DataFrame = {

    val carDF: DataFrame = {
      ss.read.option("header", "true")
        .option("inferSchema", "true")
        .format("csv")
        .load("src/main/resources/exercises/car.txt")
    }
    carDF.filter($"valid" === 1).select($"user_id", $"id", $"model")
  }

}

// 2.
object Exercise2 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  Logger.getLogger("org").setLevel(Level.ERROR)
  lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("exercise2")
  lazy val sc: SparkContext = SparkContext.getOrCreate(conf)

  def main(args: Array[String]): Unit = {

    val carRDD: RDD[String] = sc.textFile("src/main/resources/exercises/car.txt")

    val carRDDtyped: RDD[(Int, String, String, Double, Int)] = {
      carRDD
        .mapPartitionsWithIndex((idx, lines) => if (idx == 0) lines.drop(1) else lines)
        .map(line => {
          val arr = line.split(",")
          (arr(0).toInt,
            arr(1),
            arr(2),
            arr(3).toDouble,
            arr(4).toInt)
        })
    }

    val carRDDgrouped = carRDDtyped.keyBy(_._3).mapValues(v => (v._1, v._2, v._4.toInt, v._5.toInt)).groupByKey
    val avgByKey: RDD[String] = {
      carRDDtyped.keyBy(_._3)
        .mapValues(v => (v._4, 1d))
        .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
        .mapValues(v => v._1 / v._2).map {
        case (key, value) => Array(key, value).mkString(",")
      }.repartition(1)
    }

    val srcPath = "src/main/resources/exercises/answers/rawCsv2.csv"

    lazy val ss: SparkSession = SparkSession.builder().config("spark.master", "local").getOrCreate

    // This import is needed to use the $-notation
    import ss.implicits._

    saveCSV(avgByKey.toDF("type,avg(number)"), srcPath, "part2.csv")

    //avgByKey.saveAsTextFile(srcPath)
    //    merge(srcPath, dstPath)
    sc.stop

  }
}

// 3.
object Exercise3 {

  import org.apache.log4j.{Level, Logger}
  import org.apache.spark.sql.{DataFrame, SparkSession}

  Logger.getLogger("org").setLevel(Level.ERROR)
  lazy val ss: SparkSession = SparkSession.builder.appName("exercise3").config("spark.master", "local").getOrCreate()

  // This import is needed to use the $-notation
  import ss.implicits._

  def main(args: Array[String]): Unit = {
    val df: DataFrame = {
      ss.read.option("header", "true")
        .option("inferSchema", "true")
        .format("csv")
        .load("src/main/resources/exercises/car.txt")
    }

    val mean: DataFrame = df.select($"number", $"type").groupBy("type").mean()
    val max: DataFrame = df.select($"number", $"type").groupBy("type").max()
    val min: DataFrame = df.select($"number", $"type").groupBy("type").min()

    val result: DataFrame = mean.join(max, "type").join(min, "type").repartition(1)
    //result.show()

    val csvRawPath = "src/main/resources/exercises/answers/rawCsv3.csv"

    saveCSV(result, csvRawPath, "part3.csv")

    ss.stop

  }
}

class Exercise1()

class Exercise2()

class Exercise3()

case class saveCSV[Option](df: DataFrame, unputFileName: String, outputFileName: String) {

  import org.apache.hadoop.fs._

  val fs: FileSystem = FileSystem.get(new Configuration())

  FileUtil.fullyDelete(new File(unputFileName))
  fs.delete(new Path("src/main/resources/exercises/answers/" + outputFileName), true)

  df.write.csv(unputFileName)

  val file: String = fs.globStatus(new Path(unputFileName + "/part-0000*"))(0).getPath.getName

  fs.rename(new Path(unputFileName + "/" + file), new Path("src/main/resources/exercises/answers/" + outputFileName))
  fs.delete(new Path(unputFileName + "/*"), true)
  fs.delete(new Path("src/main/resources/exercises/answers/." + outputFileName + ".crc"), true)
  FileUtil.fullyDelete(new File(unputFileName))


}