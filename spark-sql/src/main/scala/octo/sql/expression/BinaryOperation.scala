package octo.sql.expression

import octo.sql.plan.physical.InternalRow

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}
import scala.util.Try

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
