package com.octo.mythbuster.spark.sql.expression

import com.octo.mythbuster.spark.compiler.JavaCode
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.physical.Row
import com.octo.mythbuster.spark.sql.parser.AST

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

// An Expression can be applied on an InternalRow in order to produce a Type
trait Expression extends AST {

  type Type

  def evaluate(row: Row): Type

  def toPredicate: Try[Predicate] = {
    Failure(new Exception(s"The ${this} expression is not a predicate"))
  }

  def generateJavaCode(javaVariableName: JavaCode): JavaCode

  def consume: Seq[TableColumn]

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

trait UnaryOperation extends Expression {

  val child: Expression

}

trait BinaryOperation extends Expression {

  val leftChild: Expression

  val rightChild: Expression

  def typed[A: TypeTag](row: Row)(eval: (A, A) => Type): Type = {
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

// Alias is a concret implementation of a NamedExpression (1 + 1 AS two)
case class Alias(child: Expression, name: ExpressionName) extends UnaryOperation with NamedExpression {

  type Type = child.Type

  override def evaluate(row: Row) = child.evaluate(row)

  override def generateJavaCode(javaVariableName: JavaCode) = child.generateJavaCode(javaVariableName)

  override def consume = child.consume

}

// And is a predicate which evaluate the &&
case class And(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "&&"

  override def evaluate(row: Row) = typed[Boolean](row) { _ &&  _ }

  override def consume = leftChild.consume ++ rightChild.consume

}

// Equal evaluate the equality of its two children
case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "FIXME"

  override def evaluate(row: Row) = typed[Any](row) { (left: Any, right: Any) =>
    left.toString.equals(right.toString)
  }

  // Because of the Any type (as we did not make a real type system), we need to convert them to String in order to do the comparaison
  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"((${leftChild.generateJavaCode(javaVariableName)}).toString().equals((${rightChild.generateJavaCode(javaVariableName)}).toString()))"
  }

  override def consume = leftChild.consume ++ rightChild.consume

}

// Greater evaluate the >
case class Greater(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = ">"

  override def evaluate(row: Row) = leftChild.evaluate(row).toString.toFloat > rightChild.evaluate(row).asInstanceOf[Float]

  // Because of the Any type (as we did not make a real type system), we need to convert them to String in order to do the comparaison
  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"Float.parseFloat(${leftChild.generateJavaCode(javaVariableName)}.toString()) > ${rightChild.generateJavaCode(javaVariableName)}"
  }

  override def consume = leftChild.consume ++ rightChild.consume

}

// Less evaluate the <
case class Less(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "<"

  override def evaluate(row: Row) = typed[Float](row) { _ < _ }

  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"Float.parseFloat(${leftChild.generateJavaCode(javaVariableName)}.toString()) < ${rightChild.generateJavaCode(javaVariableName)}"
  }

  override def consume = leftChild.consume ++ rightChild.consume

}

sealed trait Literal extends Expression {

  val value: Type

  override def evaluate(row: Row): Type  = value

}

// Text literal, evaluated as String
case class Text(value: String) extends Literal {

  override type Type = String

  def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s""""${value}""""
  }

  override def consume = Seq()

}

// Number literal, evaluated as Float
case class Number(value: Float) extends Literal {

  override type Type = Float

  def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"Float.valueOf(${value}f)"
  }

  override def consume = Seq()

}

case class Bool(value: Boolean) extends Literal {

  override type Type = Boolean

  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"Boolean.valueOf(${value})"
  }

  override def consume = Seq()

}

case class Or(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "||"

  override def evaluate(row: Row) = typed[Boolean](row) { _ || _ }

  override def consume = leftChild.consume ++ rightChild.consume

}

// A TableColumn is a NamedExpression which modelize the fact of accessing the data of a table by its column name (t1.name)
case class TableColumn(columnName: ColumnName) extends Expression with NamedExpression {

  val name = columnName

  override type Type = Any

  override def evaluate(row: Row): Type = {
    row(columnName)
  }

  override def generateJavaCode(javaVariableName: JavaCode): JavaCode = {
    s"""${javaVariableName}.getValue("${columnName}")"""
  }

  override def consume = Seq(this)

}
