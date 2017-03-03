import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.execution.debug._

/**
  * Created by adrien on 2/13/17.
  */
object ProveToMarcThatHeIsWrong {

  case class Car(id: Int, name: String, speed: Double, companyID: Int)
  case class Company(id: Int, name: String)

  def main(arguemts: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.DEBUG)

    val sparkSession = SparkSession.builder()
          .master("local[*]")
          .appName("Cars")
      .getOrCreate()

    sparkSession
      .createDataFrame(Seq(Car(1, "Celica", 25d, 1), Car(2, "Clio", 23d, 2)))
      .createOrReplaceTempView("cars")

    sparkSession
      .createDataFrame(Seq(Company(1, "Toyota"), Company(2, "Renault")))
      .createOrReplaceTempView("companies")

    val cars = sparkSession.sql("SELECT * FROM cars ca, companies co WHERE ca.companyID = co.id")

    cars.debugCodegen()

    cars.explain(true)

    cars.show()
  }

}

