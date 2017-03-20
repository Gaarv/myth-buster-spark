package octo.sql.expression

import scala.util.Try

case class Greater(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = ">"

  override def evaluate(row: InternalRow) = typed[Float](row) { _ > _ }

}
