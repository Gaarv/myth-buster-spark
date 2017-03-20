package octo.sql.expression

import scala.util.{Failure, Try}

trait Expression extends AST {

  type Type

  def evaluate(row: InternalRow): Type

  def toPredicate: Try[Predicate] = {
    Failure(new Exception(s"The ${this} expression is not a predicate"))
  }

  def generateCode(javaVariableName: String): String

}

object NamedExpression {

  def unapply(e: Expression): Option[ColumnName] = e match {
    case TableColumn(tableName, columnName) => Some(s"${tableName}.${columnName}")
    case _ => None
  }

}
