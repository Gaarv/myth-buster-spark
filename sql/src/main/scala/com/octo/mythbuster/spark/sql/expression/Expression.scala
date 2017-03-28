package com.octo.mythbuster.spark.sql.expression

import com.octo.mythbuster.spark.compiler.JavaCode
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.physical.InternalRow
import com.octo.mythbuster.spark.sql.parser.AST

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

// An Expression can be applied on an InternalRow in order to produce a Type
trait Expression extends AST {

  type Type

  def evaluate(row: InternalRow): Type

  def toPredicate: Try[Predicate] = {
    Failure(new Exception(s"The ${this} expression is not a predicate"))
  }

  def generateJavaCode(javaVariableName: JavaCode): JavaCode

}

// A Predicate is a particular type of Expression which will produce a Boolean when applied on an InternalRow
trait Predicate extends Expression {

  override type Type = Boolean

  override def toPredicate: Try[Predicate] = Success(this)

}

// A NamedExpression can be any kind of Expression having a name
object NamedExpression {

  // This method is quite useful for pattern matching
  def unapply(expression: Expression): Option[ExpressionName] = {
    expression match {
      case namedExpression: NamedExpression => Some(namedExpression.name)
      case _ => None
    }
  }

}

trait NamedExpression {

  val name: ExpressionName

}

// Alias is a concret implementation of a NamedExpression (1 + 1 AS two)
case class Alias(child: Expression, name: ExpressionName) extends UnaryOperation with NamedExpression {

  type Type = child.Type

  override def evaluate(row: InternalRow) = child.evaluate(row)

  override def generateJavaCode(javaVariableName: JavaCode) = child.generateJavaCode(javaVariableName)

}

// And is a predicate which evaluate the &&
case class And(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "&&"

  override def evaluate(row: InternalRow) = typed[Boolean](row) { _ &&  _ }

}

trait UnaryOperation extends Expression {

  val child: Expression

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

  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"(${leftChild.generateJavaCode(javaVariableName)} ${javaOperator} ${rightChild.generateJavaCode(javaVariableName)})"
  }

}

// Equal evaluate the equality of its two children
case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "FIXME"

  override def evaluate(row: InternalRow) = typed[Any](row) { (left: Any, right: Any) =>
    left.toString.equals(right.toString)
  }

  // Because of the Any type (as we did not make a real type system), we need to convert them to String in order to do the comparaison
  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"((${leftChild.generateJavaCode(javaVariableName)}).toString().equals((${rightChild.generateJavaCode(javaVariableName)}).toString()))"
  }

}

// Greater evaluate the >
case class Greater(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = ">"

  override def evaluate(row: InternalRow) = typed[Float](row) { _ > _ }

}

// Less evaluate the <
case class Less(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "<"

  override def evaluate(row: InternalRow) = typed[Float](row) { _ < _ }

}

sealed trait Literal extends Expression {

  val value: Type

  override def evaluate(row: InternalRow): Type  = value

}

// Text literal, evaluated as String
case class Text(value: String) extends Literal {

  override type Type = String

  def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s""""${value}""""
  }

}

// Number literal, evaluated as Float
case class Number(value: Float) extends Literal {

  override type Type = Float

  def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"Float.valueOf(${value}f)"
  }

}

case class Or(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "||"

  override def evaluate(row: InternalRow) = typed[Boolean](row) { _ || _ }

}

// A TableColumn is a NamedExpression which modelize the fact of accessing the data of a table by its column name (t1.name)
case class TableColumn(relationName: RelationName, columnName: ColumnName) extends Expression with NamedExpression {

  override type Type = Any

  val name = columnName

  override def evaluate(row: InternalRow): Type = row((Some(relationName), columnName))

  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"""${javaVariableName}.getValue(TableNameAndColumnName.of(Optional.of("${relationName}"), "${columnName}"))"""
  }

}
