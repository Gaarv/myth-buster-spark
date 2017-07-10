package com.octo.mythbuster.spark.sql

package object plan {

  trait Rule[PlanType <: Plan[PlanType]] extends (PlanType => PlanType) {

    val name: String = "Unamed"

  }

}
