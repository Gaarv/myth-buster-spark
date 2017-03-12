package octo.sql.plan.physical

import octo.sql.{Row, TableName}
import octo.sql.{expression => e}
import octo.sql._
import octo.sql.plan.physical.codegen.CodeGenerator

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait PhysicalPlan {

  def execute(): Iterator[InternalRow]

}

case class TableScan(tableName: TableName, iterable: Iterable[Row]) extends PhysicalPlan {

  override def execute(): Iterator[InternalRow] = iterable.iterator.map(_.toInternalRow(tableName))
}

case class Filter(child: PhysicalPlan, expression: e.Expression) extends PhysicalPlan with CodeGenerator {

  override def execute() : Iterator[InternalRow] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

  override def generateCode(parentCode: String): String =
    s"""
      |InternalRow firstRow = getCurrentRows().getFirst();
      |if(!(${expression.generateCode("firstRow")})) {
      |  getCurrentRows().pop();
      |}
      |else {
      |  $parentCode
      |}
    """.stripMargin

}

case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends PhysicalPlan {

  override def execute(): Iterator[InternalRow] = for {
    leftRow <- leftChild.execute()
    rightRow <- rightChild.execute().toSeq
  } yield leftRow ++ rightRow
}

case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with CodeGenerator {

  def execute(): Iterator[InternalRow] = child.execute().map({ physicalRow: InternalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value = expression.evaluate(physicalRow)
      (expression match {
        case e.NamedExpression(expressionName) => (None, expressionName)
        case _ => (None, s"column_${index}")
      }) -> value

    }): _*)
  })

  override def generateCode(parentCode: String): String =
    s"""
      |InternalRow internalRowForProjection = currentRows.getFirst();
      |Object valuesForProjection[] = {
      |  ${expressions.map(_.generateCode("internalRowForProjection")).mkString(",\n")}
      |};
      |Row rowForProjection = Row.empty();
      |for(int index = 0; index < valuesForProjection.length; index++) {
      |  rowForProjection.setValue("column_" + index, valuesForProjection[index]);
      |}
      |currentRows.pop();
      |currentRows.add(rowForProjection);
    """.stripMargin

}
