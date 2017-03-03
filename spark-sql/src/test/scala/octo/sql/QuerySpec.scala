package octo.sql

import octo.UnitSpec

import scala.util.{Failure, Success}

class QuerySpec extends UnitSpec {

  "The query" should "work" in {

    case class Car(id: Int, name: String, companyID: Int)

    def tableOfCar = table[Car]("cars") { case Car(id, name, companyID) => Map("id" -> id, "name" -> name, "company_id" -> companyID) }

    val cars = Seq(
      Car(1, "Twingo", 1),
      Car(2, "Clio", 1),
      Car(3, "Prius", 2),
      Car(4, "Celica", 2)
    )

    case class Company(id: Int, name: String)

    def tableOfCompany = table[Company]("companies") { case Company(id, name) => Map("id" -> id, "name" -> name) }

    val companies = Seq(
      Company(1, "Renault"),
      Company(2, "Toyota")
    )

    implicit val tableRegistry: TableRegistry = tableOfCar(cars) ++ tableOfCompany(companies)

    val sql =
      """
        |SELECT
        |  cars.name
        |FROM
        |  cars
        |JOIN
        |  companies
        |ON
        |  cars.company_id = companies.id
        |WHERE
        |  companies.name = 'Toyota'
      """.stripMargin

    Query(sql) match {
      case Success(query) => query.fetch().foreach(println)
      case Failure(e) => fail(e)
    }
  }

}
