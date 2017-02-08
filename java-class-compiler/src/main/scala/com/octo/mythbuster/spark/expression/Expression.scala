package com.octo.mythbuster.spark.expression

import com.octo.mythbuster.spark.Row

trait Expression {

  type Type

  def evaluate(row: Row): Type

}
