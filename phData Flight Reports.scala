// Databricks notebook source
// imports
import scala.collection.mutable
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{asc, desc, col, lit, sum, udf}
import org.apache.spark.broadcast.Broadcast

val sqlContext = SQLContext.getOrCreate(sc)
import sqlContext.implicits._

// COMMAND ----------

// general helper functions
def readCSV(path: String): DataFrame = {
  spark
    .read
    .option("inferSchema", false)
    .option("header", true)
    .csv(s"/FileStore/tables/shaddoxac/$path")
}


def createWidgetFromPossibleValues(df: DataFrame, columnName: String, extraValues: String*): Unit = {
  val distinctValues: Seq[String] = {
    df
      .select(columnName)
      .distinct.orderBy(col(columnName).asc)
      .as[String]
      .collect
      .toSeq
  }
  
  val possibleValues: Seq[String] = extraValues ++ distinctValues
  
  dbutils
    .widgets
    .dropdown(
      columnName,
      possibleValues.head,
      possibleValues
    )
}

def createBroadcastedMap(df: DataFrame, keyColumnName: String, valueColumnName: String): Broadcast[Map[String, String]] = {
  sc.broadcast(
    df
      .map{row =>
        row.getAs[String](keyColumnName) -> row.getAs[String](valueColumnName)
      }
      .collect
      .toMap
  )
}

// COMMAND ----------

// input file names
val airportsFilePath = "airports-1.csv"
val airlinesFilePath = "airlines-1.csv"
val flightsFilePath = "flights/*"

// COMMAND ----------

// read in input data
val airportsDF = readCSV(airportsFilePath).persist(StorageLevel.MEMORY_ONLY_SER)
val airlinesDF = readCSV(airlinesFilePath).persist(StorageLevel.MEMORY_ONLY_SER)  
val flightsDF = readCSV(flightsFilePath).persist(StorageLevel.MEMORY_ONLY_SER)

val airportsMap = createBroadcastedMap(airportsDF, "IATA_CODE", "AIRPORT")
val airlinesMap = createBroadcastedMap(airlinesDF, "IATA_CODE", "AIRLINE")

// COMMAND ----------

// domain specific helper functions
def airportCodeToName(iataCode: String): String = {
  airportsMap.value.get(iataCode).getOrElse("Invalid IATA Code")
}
val udfAirportCodeToName = udf{(iataCode: String) => airportCodeToName(iataCode)}

def airlineCodeToName(iataCode: String): String = {
  airlinesMap.value.get(iataCode).getOrElse("Invalid IATA Code")
}
val udfAirlineCodeToName = udf{(iataCode: String) => airlineCodeToName(iataCode)}

// COMMAND ----------

dbutils.widgets.removeAll()

// COMMAND ----------

// set up helper widgets
createWidgetFromPossibleValues(flightsDF, "YEAR", "ALL")
createWidgetFromPossibleValues(flightsDF, "MONTH", "ALL")
createWidgetFromPossibleValues(airlinesDF, "AIRLINE", "ALL")
createWidgetFromPossibleValues(airportsDF, "AIRPORT", "ALL")

// COMMAND ----------

def createWidgetFilters(inputMapping: Map[String, String]): Column = {  
  inputMapping
    .foldLeft(lit(true)){(currentStatus, newConditionTup) =>
      val (columnName, widgetName) = newConditionTup
      val currentArgument = dbutils.widgets.get(widgetName)
      val newConditionColumn = {
        if (currentArgument == "ALL") lit(true)
        else                          col(columnName).equalTo(currentArgument)
      }

      currentStatus and newConditionColumn
    }
} 

// COMMAND ----------

// MAGIC %md
// MAGIC ## Total number of flights by airline and airport on a monthly basis

// COMMAND ----------

val flightsByAirlineAndAirport = {
  flightsDF
    .groupBy("YEAR", "MONTH", "AIRLINE", "ORIGIN_AIRPORT")
    .count
    .select(
      udfAirlineCodeToName($"AIRLINE") as "AIRLINE",
      udfAirportCodeToName($"ORIGIN_AIRPORT") as "ORIGIN_AIRPORT",
      $"YEAR",
      $"MONTH",
      $"count" as "COUNT"
    )
}.persist(StorageLevel.MEMORY_ONLY_SER)

// COMMAND ----------

display(
  flightsByAirlineAndAirport
    .where(createWidgetFilters(Map("YEAR" -> "YEAR", "MONTH" -> "MONTH", "AIRLINE" -> "AIRLINE", "ORIGIN_AIRPORT" -> "AIRPORT")))
    .orderBy($"YEAR".asc, $"MONTH".asc, $"AIRLINE".asc, $"ORIGIN_AIRPORT".asc)
)

// COMMAND ----------

if (dbutils.widgets.get("AIRPORT") == "ALL") {
  if (dbutils.widgets.get("AIRLINE") != "ALL") {
    display(
      flightsByAirlineAndAirport
        .drop("AIRLINE")
        .where(createWidgetFilters(Map("YEAR" -> "YEAR", "MONTH" -> "MONTH", "ORIGIN_AIRPORT" -> "AIRPORT")))
        .orderBy($"YEAR".asc, $"MONTH".asc, $"AIRLINE".asc, $"ORIGIN_AIRPORT".asc)
    )
  }
}

// COMMAND ----------

if (dbutils.widgets.get("AIRLINE") == "ALL") {
  if (dbutils.widgets.get("AIRPORT") != "ALL") {
    display(
      flightsByAirlineAndAirport
        .drop("ORIGIN_AIRPORT")
        .where(createWidgetFilters(Map("YEAR" -> "YEAR", "MONTH" -> "MONTH", "AIRLINE" -> "AIRLINE")))
        .orderBy($"YEAR".asc, $"MONTH".asc, $"AIRLINE".asc)
    )
  }
}
else if (dbutils.widgets.get("AIRPORT") == "ALL") {
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## On time percentage of each airline for the year 2015

// COMMAND ----------

case class OnTimeReport(AirlineName: String, TotalDelayCount: Int, TotalFlightCount: Int, DelayPercentage: Double)

// COMMAND ----------

val onTimePercentageByAirline = {
  flightsDF
    .flatMap{ row =>
      if (row.getAs[String]("YEAR") == "2015") {
        val iataCode = row.getAs[String]("AIRLINE")
        
        val isDelayed = {
          val departureDelay = row.getAs[String]("DEPARTURE_DELAY")
          
          if (departureDelay != null && departureDelay.nonEmpty) {
            departureDelay.toInt > 0
          }
          else {
            val arrivalDelay = row.getAs[String]("ARRIVAL_DELAY")
            
            if (arrivalDelay != null && arrivalDelay.nonEmpty) {
              arrivalDelay.toInt > 0
            }
            else {
              false
            }
          }
        }

        Some(iataCode -> (if (isDelayed) 1 else 0, 1))
      }
      else {
        None
      }
    }
    .rdd
    .reduceByKey{ (tup1, tup2) =>
      (tup1._1 + tup2._1, tup1._2 + tup2._2)
    }
    .map{ case (iata_code, (delayCount, totalCount)) =>
      OnTimeReport(
        AirlineName      = airlineCodeToName(iata_code),
        TotalDelayCount  = delayCount,
        TotalFlightCount = totalCount,
        DelayPercentage  = delayCount.toDouble / totalCount
      )
    }
    .toDS
}.persist(StorageLevel.MEMORY_ONLY_SER)

// COMMAND ----------

display(
    onTimePercentageByAirline
      .orderBy($"DelayPercentage".asc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Airlines with the largest number of delays

// COMMAND ----------

val delaysByAirline = {
  flightsDF
    .flatMap{ row =>
      val isDelayed = {
        val departureDelay = row.getAs[String]("DEPARTURE_DELAY")

        if (departureDelay != null && departureDelay.nonEmpty) {
          departureDelay.toInt > 0
        }
        else {
          val arrivalDelay = row.getAs[String]("ARRIVAL_DELAY")

          if (arrivalDelay != null && arrivalDelay.nonEmpty) {
            arrivalDelay.toInt > 0
          }
          else {
            false
          }
        }
      }
      
      if (isDelayed) {
        val airlineName = airlineCodeToName(row.getAs[String]("AIRLINE"))
        Some(airlineName -> 1)
      }
      else {
        None
      }
    }
    .rdd
    .reduceByKey{ _ + _ }
    .toDF("AirlineName", "NumDelays")
}.persist(StorageLevel.MEMORY_ONLY_SER)

// COMMAND ----------

display(
  delaysByAirline
    .where(createWidgetFilters(Map("AirlineName" -> "AIRLINE")))
    .orderBy($"NumDelays".desc)
)

// COMMAND ----------

display(
  delaysByAirline
    .orderBy($"NumDelays".desc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cancellation reasons by airport

// COMMAND ----------

// helper objects for determining cancellation by reason
case class CancellationReport(airportName: String, totalCancellations: Int, AirlineCarrierCancellations: Int, WeatherCancellations: Int, NationalAirSystemCancellations: Int, SecurityCancellations: Int)

// we can save some space/time by having the reasons represented as ints
final val AIRLINE_CARRIER_CANCELLATION     = 0
final val WEATHER_CANCELLATION             = 1
final val NATIONAL_AIR_SYSTEM_CANCELLATION = 2
final val SECURITY_CANCELLATION            = 3

final val NUM_CANCELLATION_REASONS = 4

def charCancellationReasonToInt(c: Char): Int = {
  c.toInt - 65 // use char mapping
}

// COMMAND ----------

val cancellationReasonsByAirport = {
  flightsDF
    .flatMap{ row =>
      val cancellationReason = row.getAs[String]("CANCELLATION_REASON")
      
      if (cancellationReason != null && cancellationReason.nonEmpty) {        
        val intCancellationReason = charCancellationReasonToInt(cancellationReason.head)

        val reasonArray = mutable.ArrayBuffer.fill(NUM_CANCELLATION_REASONS)(0)
        reasonArray(intCancellationReason) = 1
        
        Some(row.getAs[String]("ORIGIN_AIRPORT") -> reasonArray)
      }
      else {
        None
      }
    }
    .rdd
    .reduceByKey{ (ar1, ar2) =>
      (0 until NUM_CANCELLATION_REASONS).foreach{ index =>
        ar1(index) += ar2(index)
      }
      
      ar1
    }
    .map{case (airlineCode, reasonArray) =>
      CancellationReport(
        airportName                    = airportCodeToName(airlineCode),
        totalCancellations             = reasonArray.sum,
        AirlineCarrierCancellations    = reasonArray(AIRLINE_CARRIER_CANCELLATION),
        WeatherCancellations           = reasonArray(WEATHER_CANCELLATION),
        NationalAirSystemCancellations = reasonArray(NATIONAL_AIR_SYSTEM_CANCELLATION),
        SecurityCancellations          = reasonArray(SECURITY_CANCELLATION)
      )
    }
    .toDF
}.persist(StorageLevel.MEMORY_ONLY_SER)

// COMMAND ----------

display(
  cancellationReasonsByAirport
    .where(createWidgetFilters(Map("airportName" -> "AIRPORT")))
    .orderBy($"airportName".asc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Delay reasons by airport

// COMMAND ----------

val delayReasonsByAirport = {
  flightsDF
    .groupBy("ORIGIN_AIRPORT")
    .agg(sum("AIR_SYSTEM_DELAY"), sum("SECURITY_DELAY"), sum("AIRLINE_DELAY"), sum("LATE_AIRCRAFT_DELAY"), sum("WEATHER_DELAY"))
    .withColumn("ORIGIN_AIRPORT", udfAirportCodeToName($"ORIGIN_AIRPORT"))
    .toDF("AIRPORT", "AIR_SYSTEM_DELAY_COUNT", "SECURITY_DELAY_COUNT", "AIRLINE_DELAY_COUNT", "LATE_AIRCRAFT_DELAY_COUNT", "WEATHER_DELAY_COUNT")
}.persist(StorageLevel.MEMORY_ONLY_SER)

// COMMAND ----------

display(
  delayReasonsByAirport
    .where(createWidgetFilters(Map("AIRPORT" -> "AIRPORT")))
    .orderBy($"AIRPORT".asc)
)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Airline with the most unique routes

// COMMAND ----------

val uniqueRoutesByAirline = {
  flightsDF
    .map{ row =>
      val iataCode = row.getAs[String]("AIRLINE")
      val origin = row.getAs[String]("ORIGIN_AIRPORT")
      val destination = row.getAs[String]("DESTINATION_AIRPORT")
      
      iataCode -> Set(origin -> destination)
    }
    .rdd
    .reduceByKey{ _ union _ }
    .map{ case (iataCode, uniqueRouteSet) =>
      val airlineName = airlineCodeToName(iataCode)
      val numUniqueRoutes = uniqueRouteSet.size
      
      airlineName -> numUniqueRoutes
    }
    .toDF("AirlineName", "UniqueRoutes")
}.persist(StorageLevel.MEMORY_ONLY_SER)

// COMMAND ----------

display(
  uniqueRoutesByAirline
    .orderBy($"UniqueRoutes".desc)
)

// COMMAND ----------


