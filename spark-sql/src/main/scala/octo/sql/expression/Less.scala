package octo.sql.expression

case class Less(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "<"

  override def evaluate(row: InternalRow) = typed[Float](row) { _ < _ }

}
