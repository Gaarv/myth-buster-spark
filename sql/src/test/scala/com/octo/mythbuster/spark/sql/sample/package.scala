package com.octo.mythbuster.spark.sql

package object sample {

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

  implicit val rowIterableRegistry: RowIterableRegistry = /*tableOfCar(cars) ++ */tableOfCompany(companies)

}
