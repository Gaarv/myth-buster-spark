package octo.sql.expression

case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override val javaOperator: String = "FIXME"

  override def evaluate(row: InternalRow) = typed[Any](row) { (left: Any, right: Any) => left == right }

  override def generateCode(javaVariableName: String): String = {
    s"(${leftChild.generateCode(javaVariableName)}.equals(${rightChild.generateCode(javaVariableName)}))"
  }

}
