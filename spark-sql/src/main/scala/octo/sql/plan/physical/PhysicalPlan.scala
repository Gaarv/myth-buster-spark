package octo.sql.plan.physical

import octo.sql.{Row, TableName}
import octo.sql.{expression => e}
import octo.sql._
import octo.GeneratedIterator
import octo.sql.plan.physical.codegen.CodeGenerator

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class PhysicalPlan(projection: Projection) {

  def execute(): Iterator[Row] = projection.execute()

}

trait Stage {

  def execute(): Iterator[InternalRow]

}

case class TableScan(tableName: TableName, iterable: Iterable[Row]) extends Stage {

  override def execute(): Iterator[InternalRow] = iterable.iterator.map(_.toInternalRow(tableName))
}

case class Filter(child: Stage, expression: e.Expression) extends Stage with CodeGenerator {

  override def execute() : Iterator[InternalRow] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

  override def generateCode(parentCode: String): String =
    """
      |InternalRow firstRow = getCurrentRows().getFirst();
      |if(!(${expression.generateCode("firstRow")})) {
      |  getCurrentRows().pop();
      |}
      |else {
      |  $parentCode
      |}
    """.stripMargin

}

case class CartesianProduct(leftChild: Stage, rightChild: Stage) extends Stage {

  override def execute(): Iterator[InternalRow] = for {
    leftRow <- leftChild.execute()
    rightRow <- rightChild.execute().toSeq
  } yield leftRow ++ rightRow
}

case class Projection(child: Stage, expressions : Seq[e.Expression]) extends CodeGenerator {

  def execute(): Iterator[Row] = child.execute().map({ physicalRow: InternalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value = expression.evaluate(physicalRow)
      s"column_${index}" -> value
    }): _*)
  })

  override def generateCode(parentCode: String): String = {

  }

}
