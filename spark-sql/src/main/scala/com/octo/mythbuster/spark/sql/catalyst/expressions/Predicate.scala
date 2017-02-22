package com.octo.mythbuster.spark.sql.catalyst.expressions

trait Predicate extends Expression {

  override type Type = Boolean

  def toString(inputName : String) : String

}
