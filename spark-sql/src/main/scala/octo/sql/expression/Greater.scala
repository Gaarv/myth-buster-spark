package octo.sql.expression

import octo.sql.plan.physical.InternalRow

import scala.util.Try

case class Greater(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override def evaluate(row: InternalRow) = typed[Float](row) { _ > _ }

}
