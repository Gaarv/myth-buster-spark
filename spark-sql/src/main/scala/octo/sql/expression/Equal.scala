package octo.sql.expression

import octo.sql.plan.physical.InternalRow

case class Equal(leftChild: Expression, rightChild: Expression) extends BinaryOperation with Predicate {

  override def evaluate(row: InternalRow) = typed[Any](row) { (left: Any, right: Any) => left == right }

  //override def toString(inputName : String) = leftChild.toString(inputName) + ".equals(" + rightChild.toString(inputName) + ")"

}
