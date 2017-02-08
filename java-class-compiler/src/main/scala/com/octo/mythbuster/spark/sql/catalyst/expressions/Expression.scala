package com.octo.mythbuster.spark.sql.catalyst.expressions

import com.octo.mythbuster.spark.sql.Row

trait Expression {

  type Type

  def evaluate(row: Row): Type

}
