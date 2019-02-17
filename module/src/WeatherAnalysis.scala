import java.io.{BufferedReader, IOException, InputStreamReader}
import java.net.{MalformedURLException, ConnectException, URL}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SparkSession

case class Historical_Measures(station_name: String, year: Int, mm: Int, tmax_degC: Float, tmin_degC: Float,
                               af_days: Int, rain_mm: Float, sun_hours: Float)

object WeatherAnalysis extends App {
  @throws(classOf[IOException])
  @throws(classOf[MalformedURLException])
  @throws(classOf[ConnectException])
  def read_file_from_url(station_name: String, url_string: String): ListBuffer[String] = {
    val url = new URL(url_string)
    val file_with_line_contents_buf: ListBuffer[String] = ListBuffer()
    val line_contents = new BufferedReader(new InputStreamReader(url.openStream()))
    var line = line_contents.readLine
    var start_buffering: Boolean = false
    val k_ref_string: String = "hours"
    val k_ignore_site_closed_line: String = "Site Closed"
    val k_commas: Int = 6
    while (line != null) {
      //cleanse the data by replacing *,#$ and Characters with empty string
      //replace one or more space with single comma
      //replace --- [missing data] with 0 char to avoid arrayIndexOutOfBoundException .
      if (line.contains(k_ref_string) && start_buffering == false) {
        start_buffering = true
      }
      else {
        if (start_buffering && !line.equalsIgnoreCase(k_ignore_site_closed_line)) {
          line = line.trim.replaceAll("[a-zA-Z]", "").replaceAll("[#|*|\\$]", "").replaceAll("( )+", ",").replaceAll("---", "0")
          //add zero string to match 7 elements of data file
          val no_of_commas = line.count(_ == ',')
          if (no_of_commas < k_commas) {
            line = line + ",0" * (k_commas - no_of_commas)
          }
          file_with_line_contents_buf.+=(station_name + "," + line)
        }
      }
      line = line_contents.readLine()
    }
    line_contents.close
    file_with_line_contents_buf
  }

  val base_url_string: String = "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/"
  val station_list: List[String] = List("aberporth", "armagh", "ballypatrick", "bradford", "braemar", "camborne",
    "cambridge", "cardiff", "chivenor", "cwmystwyth", "dunstaffnage", "durham",
    "eastbourne", "eskdalemuir", "heathrow", "hurn", "lerwick", "leuchars", "lowestoft",
    "manston", "nairn", "newtonrigg", "oxford", "paisley", "ringway",
    "rossonwye", "shawbury", "sheffield", "southampton", "stornoway", "suttonbonington",
    "tiree", "valley", "waddington", "whitby", "wickairport", "yeovilton")

  //val station_list:List[String]=List("lowestoft")

  val startTimeMillis = System.currentTimeMillis()
  var station_data_list_Buf: ListBuffer[String] = ListBuffer()

  for (i <- station_list) {
    //val startTimeMillis = System.currentTimeMillis()
    try {
      for (j <- read_file_from_url(i, base_url_string + i + "data.txt")) {
        station_data_list_Buf += j
      }
      //val durationSeconds = (System.currentTimeMillis()-startTimeMillis)/1000
      //println(durationSeconds+" for "+i+" station")
    }
    catch {
      case ex: IOException => println(ex + " for " + i + " station")
      case ex: MalformedURLException => println(ex + " for " + i + " station")
      case ex: ConnectException => println(ex + " for " + i + " station")
      case ex: Exception => println(ex + " for " + i + " station")
    }
  }

  val station_data_list: List[String] = station_data_list_Buf.toList
  val durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000
  println("Total Data Create time :" + durationSeconds+" secs. Total No of List Elements :" + station_data_list.length)

  //station_data_list_Buf.foreach(println)

  /*val conf= new SparkConf()
  conf.set("spark.executor.memory", "2g")
  conf.set("spark.driver.memory","2g")*/

  val spark_session = SparkSession.builder().appName("UK Historical Weather Data Analysis").master("local[4]").getOrCreate()

  //spark_session.sparkContext.setLogLevel("Error")
  //spark_session.conf.set("spark.executor.memory", "2g")
  //spark_session.conf.set("spark.driver.memory","2g")

  spark_session.conf.set("spark.sql.shuffle.partitions", 4)

  val df = spark_session.createDataFrame(station_data_list.map(x => x.split(",")).
    map(x => Historical_Measures(x(0), x(1).toInt, x(2).toInt, x(3).toFloat, x(4).toFloat, x(5).toInt, x(6).toFloat, x(7).toFloat)))

  //println(df.rdd.getNumPartitions)
  //df.explain()
  //df.show()

  df.createOrReplaceTempView("Data_Source")

  //Rank Stations they have been online
  spark_session.sql("select station_name ,cnt,dense_rank()over(order by cnt desc) as rank from " +
    "(select station_name,count(1) as cnt from Data_Source group by station_name)").show(37)

  //Rank stations by rainfall and-or sunshine
  spark_session.sql("select station_name ,rain_mm,sun_hours,dense_rank()over(order by rain_mm desc) as rank_rain_fall, " +
    "rank()over(order by sun_hours desc) as rank_sun_hours " +
    "from (select station_name,sum(rain_mm) as rain_mm,sum(sun_hours)as sun_hours from Data_Source group by station_name)").show(37)

  //Worst rainfall and best sunshine for each station with year
  spark_session.sql("select station_name, year,rain_mm,sun_hours from (" +
    " select station_name, year,rain_mm,sun_hours," +
    " min(rain_mm)over(partition by station_name) min_rain_mm,max(sun_hours)over(partition by station_name) max_sun_hours from (" +
    " select station_name,year, sum(rain_mm)rain_mm,sum(sun_hours)sun_hours " +
    " from Data_Source where rain_mm > 0 and sun_hours >0 group by station_name,year))" +
    " where (rain_mm = min_rain_mm or sun_hours = max_sun_hours)").show(100)

  //Averages for month of may across all stations,best and worst year
  spark_session.sql("select * from (" +
    " select year,avg_sun_hours,max(avg_sun_hours)over() as max_avg_yr_sun_hours,min(avg_sun_hours)over() as min_avg_yr_sun_hours" +
    " from (select year,avg(sun_hours) as avg_sun_hours from Data_Source where mm=5 and sun_hours >0 group by year)" +
    " ) where (avg_sun_hours = max_avg_yr_sun_hours or avg_sun_hours= min_avg_yr_sun_hours)").show(10)

  spark_session.stop
}