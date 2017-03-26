package com.octo.mythbuster.spark.sql

package object plan {

  type Rule[PlanType <: Plan[PlanType]] = PlanType => PlanType

}
