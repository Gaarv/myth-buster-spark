package com.octo.mythbuster.spark.expression

trait Predicate extends Expression {

  override type Type = Boolean

}
