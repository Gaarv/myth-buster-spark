package com.octo.mythbuster.spark.sql

package object plan {

  type Rule[P <: Plan[P]] = P => P

}
