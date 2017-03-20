package octo.sql.expression

case class Or(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "||"

  override def evaluate(row: InternalRow) = typed[Boolean](row) { _ || _ }
  
}
