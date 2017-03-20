package com.octo.mythbuster.spark.sql.expression

import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.physical.InternalRow
import com.octo.mythbuster.spark.sql.parser.AST

import scala.reflect.runtime.universe._

import scala.util.{ Failure, Success, Try }

trait Expression extends AST {

  type Type

  def evaluate(row: InternalRow): Type

  def toPredicate: Try[Predicate] = {
    Failure(new Exception(s"The ${this} expression is not a predicate"))
  }

  def generateCode(javaVariableName: String): String

}

trait Predicate extends Expression {

  override type Type = Boolean

  override def toPredicate: Try[Predicate] = Success(this)

}

object NamedExpression {

  def unapply(e: Expression): Option[ColumnName] = e match {
    case TableColumn(tableName, columnName) => Some(s"${tableName}.${columnName}")
    case _ => None
  }

}

case class And(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "&&"

  override def evaluate(row: InternalRow) = typed[Boolean](row) { _ &&  _ }

}

trait BinaryOperation extends Expression {

  val leftChild: Expression

  val rightChild: Expression

  def typed[A: TypeTag](row: InternalRow)(eval: (A, A) => Type): Type = {
    val left = leftChild.evaluate(row)
    val right = rightChild.evaluate(row)

    (left, right) match {
      case (left: A, right: A) => eval(left, right)
      case _ => throw new ExpressionException(s"The type of ${leftChild} and ${rightChild} are not of type ${typeOf[A]}")
    }
  }

  override def toPredicate: Try[Predicate] = for {
    lp <- leftChild.toPredicate
    lr <- toPredicate
    p <- super.toPredicate
  } yield p

  val javaOperator: String

  override def generateCode(javaVariableName: String): String = {
    s"(${leftChild.generateCode(javaVariableName)} ${javaOperator} ${rightChild.generateCode(javaVariableName)})"
  }

}

case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "FIXME"

  override def evaluate(row: InternalRow) = typed[Any](row) { (left: Any, right: Any) => left == right }

  override def generateCode(javaVariableName: String): String = {
    s"(${leftChild.generateCode(javaVariableName)}.equals(${rightChild.generateCode(javaVariableName)}))"
  }

}

case class Greater(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = ">"

  override def evaluate(row: InternalRow) = typed[Float](row) { _ > _ }

}

case class Less(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "<"

  override def evaluate(row: InternalRow) = typed[Float](row) { _ < _ }

}


sealed trait Literal extends Expression {

  val value: Type

  override def evaluate(row: InternalRow): Type  = value

}

case class Text(value: String) extends Literal {

  override type Type = String

  def generateCode(javaVariableName: String): String = {
    s""""${value}""""
  }

}

case class Number(value: Float) extends Literal {

  override type Type = Float

  def generateCode(javaVariableName: String): String = {
    s"Float.valueOf(${value}f)"
  }

}

case class Or(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "||"

  override def evaluate(row: InternalRow) = typed[Boolean](row) { _ || _ }

}

case class TableColumn(tableName: TableName, columnName: ColumnName) extends Expression {

  override type Type = Any

  override def evaluate(row: InternalRow): Type = row((Some(tableName), columnName))

  override def generateCode(javaVariableName: String): String = {
    s"""${javaVariableName}.getValue(TableNameAndColumnName.of(Optional.of("${tableName}"), "${columnName}"))"""
  }

}
