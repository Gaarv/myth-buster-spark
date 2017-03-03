package octo.sql.plan.physical

import octo.sql.{ Row, TableName }
import octo.sql.{ expression => e }
import octo.sql._


import octo.GeneratedIterator

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class PhysicalPlan(projection: Projection) {

  def execute(): Iterator[Row] = projection.execute()

}

trait Stage {

  def execute(): Iterator[InternalRow]

}

/*trait CodeGen extends PhysicalPlan {
  def formatSource(source : util.Iterator[Row]) : util.Iterator[util.Map[String, Object]] = {
    source.asScala.map(_.map{ case ((tableName, columnName), value : AnyRef) =>
      tableName + "." + columnName -> value
    }.asJava).asJava
  }

  def generateCode(parentCode : String): (String, Iterable[Row])

  override def execute(withCodeGeneration: Boolean): Iterator[Row] = {
    if(withCodeGeneration) executeGenerated()
    else executeVolcano()
  }

  def executeGenerated(): Iterator[Row] = {
    val packageName = "com.octo.nad"
    val generatedClassSimpleName = "GeneratedObject"
    val generatedClassName = packageName + "." + generatedClassSimpleName
    val (generatedCode, source) = generateCode("")
    val code =
      s"""
        |package com.octo.nad;
        |
        |import java.util.Iterator;
        |import com.octo.nad.GeneratedIterator;
        |import java.io.IOException;
        |import java.util.Map;
        |import com.octo.mythbuster.spark.sql.catalyst.parser.TableColumn;
        |import java.util.LinkedList;
        |import java.util.HashMap;
        |
        |
        |public class $generatedClassSimpleName extends GeneratedIterator {
        |
        |   @Override
        |   protected void processNext() throws IOException {
        |     $generatedCode
        |   }
        |
        |}
      """.stripMargin

    println("### CODE : \n" + code )

    val generatedIterator : GeneratedIterator = JavacJavaClassCompiler.compile(JavaClassSpec(generatedClassName, code)).get.newInstance().asInstanceOf[GeneratedIterator]

    val formattedSource = formatSource(source.toIterator.asJava).asScala
    generatedIterator.init(formattedSource)
    new ScalaGeneratedIterator(generatedIterator)
  }
}*/

case class TableScan(tableName: TableName, iterable: Iterable[Row]) extends Stage {

  override def execute(): Iterator[InternalRow] = iterable.iterator.map(_.toPhysicalRow(tableName))
}

case class Filter(child: Stage, expression: e.Expression) extends Stage {

  override def execute() : Iterator[InternalRow] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

}

case class CartesianProduct(leftChild: Stage, rightChild: Stage) extends Stage {

  override def execute(): Iterator[InternalRow] = for {
    leftRow <- leftChild.execute()
    rightRow <- rightChild.execute().toSeq
  } yield leftRow ++ rightRow
}

case class Projection(child: Stage, expressions : Seq[e.Expression]) {

  def execute(): Iterator[Row] = child.execute().map({ physicalRow: InternalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value = expression.evaluate(physicalRow)
      s"column_${index}" -> value
    }): _*)
  })

}
