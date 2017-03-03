package octo.sql.expression

import octo.sql.parser.AST
import octo.sql.plan.physical.InternalRow

import scala.util.{Failure, Try}

trait Expression extends AST {

  type Type

  def evaluate(row: InternalRow): Type

  def toPredicate: Try[Predicate] = {
    Failure(new Exception(s"The ${this} expression is not a predicate"))
  }

}
