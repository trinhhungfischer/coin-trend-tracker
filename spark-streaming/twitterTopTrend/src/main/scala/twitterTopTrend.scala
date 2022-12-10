import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Level, Logger}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.joda.time._
import org.joda.time.DateTime
import org.joda.time.format._
import java.io.PrintWriter
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/**
 *
 */
object twitterTopTrend {
  // sentiments, hardcoded for now
  // will change it layer to read from a file
  val positive = Set(
    "upgrade",
    "upgraded",
    "long",
    "buy",
    "buying",
    "growth",
    "good",
    "gained",
    "well",
    "great",
    "nice",
    "top",
    "support",
    "update",
    "strong",
    "bullish",
    "bull",
    "highs",
    "win",
    "positive",
    "profits",
    "bonus",
    "potential",
    "success",
    "winner",
    "winning",
    "good")


  val negative = Set(
    "downgraded",
    "bears",
    "bear",
    "bearish",
    "volatile",
    "short",
    "sell",
    "selling",
    "forget",
    "down",
    "resistance",
    "sold",
    "sellers",
    "negative",
    "selling",
    "blowout",
    "losses",
    "war",
    "lost",
    "loser")

  // function to get the sentiment of a  word
  def getWordSentiment(word: String) = {
    if (positive.contains(word)) 1
    else if (negative.contains(word)) -1
    else 0
  }

  val patternWord = "\\W|\\s|\\d"
  val patternTicker = "\\$[A-Z]+".r


  // get month index
  // no good substitute in joda-time
  def getMonth(month: String) = {
    val m = month.toUpperCase() match {
      case "JAN" => 1
      case "FEB" => 2
      case "MAR" => 3
      case "APR" => 4
      case "MAY" => 5
      case "JUN" => 6
      case "JUL" => 7
      case "AUG" => 8
      case "SEP" => 9
      case "OCT" => 10
      case "NOV" => 11
      case "DEC" => 12

    }
    m.toString
  }


  // get time from date string
  def getTime(dateString: String) = {
    val str = dateString.split(' ')
    val Month = getMonth(str(1))
    val Day = str(2)
    val Year = str(5).split('"')(0)
    val time = str(3).split(':')
    val Hr = time(0)
    val Min = time(1)
    val Sec = time(2)

    (Year, Month, Day, Hr, Min, Sec)

  }


  // get week index for week calculation
  def getWeek(year: String, month: String, day: String) = {
    val date = new DateTime(year.toInt, month.toInt, day.toInt, 12, 0, 0, 0)
    date.getWeekyear().toString + '-' + date.getWeekOfWeekyear().toString
  }


  // main function
  def getResult() = {

  }


  def main(args: Array[String]): Unit = {
    println("Hello world!")
  }


}





