
package co.za.zwideheights.spark;

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.{ FileReader, FileNotFoundException, IOException }
import scala.StringBuilder
import java.io.InputStream

import java.io.{ ObjectOutputStream, ObjectInputStream }
import java.io.{ FileOutputStream, FileInputStream }

/**
 * The object generates training data using a statistical method for training the model.
 * We count the number of negative words and positive words and tally up the numbers.
 * If the number of positives is greater than the number of negative then the tweet is deemed positive.
 */
object TwitterTrainingDataProducer {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("Twitter ML Code")
  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  var outFolder = "/user/hive/warehouse/ZumaMustFall-PN-Lexicons"
  var inFolder = "/user/hive/warehouse/ZumaMustFall"
  var lang = "n/a"
  var datasetLimit = 10000
  val regexUrl = "^((http[s]?|ftp):\\/)?\\/?([^:\\/\\s]+)((\\/\\w+)*\\/)([\\w\\-\\.]+[^#?\\s]+)(.*)?(#[\\w\\-]+)?$".r
  val regexHashtag = "#([^#]+)[\\s,;]*".r

  def main(args: Array[String]) {

    println("\nThe number of application argument is " + args.length);

    if (!((args.length <= 8) && (args.length >= 2))) {

      val builder = StringBuilder.newBuilder
      builder.append("\n\nspark-submit --class co.za.zwideheights.spark.TwitterTrainingDataProducer --master local[8] --deploy-mode <deploy-mode> --conf <key>=<value> <application-jar.jar ...options... ")
      builder.append("\n\n e.g Copy and paste the command below,  please take cognisance of the jar and rdd paths. ")
      builder.append("\nspark-submit --class co.za.zwideheights.spark.TwitterTrainingDataProducer --master local[8] --conf \"spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps\" /media/sf_spark.apps/scala/spark-twitter-machine.learning-1.0.0-jar-with-dependencies.jar --in-folder /user/hive/warehouse/ZumaMustFall --out-folder /user/hive/warehouse/ZumaMustFall-PN-Lexicons  --lang en --dataset-limit 10000 \n\n\n")
      println(builder.toString());

      System.exit(1)
    }

    var i: Int = 0;
    for (arg <- args) {
      arg match {
        case "--out-folder"    => outFolder = args(i + 1).toString; println(args(i + 1).toString);
        case "--in-folder"     => inFolder = args(i + 1).toString; println(args(i + 1).toString);
        case "--lang"          => lang = args(i + 1).toString; println(args(i + 1).toString);
        case "--dataset-limit" => datasetLimit = args(i + 1).toInt; println(args(i + 1).toString);
        case default           => ;
      }
      i += 1;
    }

    println("--out-folder    " + outFolder);
    println("--in-folder     " + inFolder);
    println("--lang          " + lang);
    println("--dataset-limit " + datasetLimit + "\n\n\n");

    println("Initalizaing all tweets. HDFS query")
    val zmfTweetsDF = sqlContext.jsonFile(inFolder)
    val zmfTweetsDF3 = zmfTweetsDF.select("id", "lang", "text")
    println("Tweets initialised. Selected id, lang and text")

    try {

      /* For every tweet  */
      val tweetsWithRatings = zmfTweetsDF3.map(x =>
        {
          try {
            val sparkUtil = SparkUtil;
            val preliminaryRes = sparkUtil.processTuple(x(2).toString)
            (x(0), x(1), preliminaryRes._1, preliminaryRes._2, preliminaryRes._3, preliminaryRes._4, preliminaryRes._5, preliminaryRes._6)
          } catch {
            case runtime: Exception => {
              println("Run time error. " + runtime)
              ("-1", "-1", "-1", "-1", "-1", "-1", "-1", "-1")
            }
          }
        }).filter( y => ((y._4 != "-1") && (y._7 != "") && (y._7 != null) && (y._6 != null)) ).map( tuple => "%s,%s,%s,%s,%d,%d,%d,%b".format(tuple._1, tuple._2, tuple._3,tuple._4, tuple._5, tuple._6, tuple._7, tuple._8) ).saveAsTextFile(outFolder)
      //}).filter( y => ((y._3 != "-1") && (y._7 != "") && (y._7 != null) && (y._6 != null)) ).map( tuple => "\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\",\"%b\"".format(tuple._1, tuple._2, tuple._3,tuple._4, tuple._5, tuple._6, tuple._7, tuple._8) ).saveAsTextFile(outFolder)
      println(s"The tweets with sentiment prediction were saved in folder $outFolder in hdfs.")
      //tweetsWithRatings.write.format("parquet").mode(SaveMode.Append).saveAsTable("zumamustfall_db.zmf_tweets_stats")

    } catch {
      case ex: FileNotFoundException => println(ex)
      case ex: IOException => println(ex)
      System.exit(1);
    }

  }

}
