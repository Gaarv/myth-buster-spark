package octo.sql.expression

import octo.sql.plan.physical.InternalRow

sealed trait Literal extends Expression {

  val value: Type

  override def evaluate(row: InternalRow): Type  = value

}

case class Text(value: String) extends Literal {

  override type Type = String

  def generateCode(javaVariableName: String): String =
    s"""
      |"${value}"
    """.stripMargin

}

case class Number(value: Float) extends Literal {

  override type Type = Float

  def generateCode(javaVariableName: String): String =
    s"""
       |${value}f
    """.stripMargin

}