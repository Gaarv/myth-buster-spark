package octo.sql.expression

import octo.sql.plan.physical.InternalRow

case class Or(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "||"

  override def evaluate(row: InternalRow) = typed[Boolean](row) { _ || _ }
  
}
