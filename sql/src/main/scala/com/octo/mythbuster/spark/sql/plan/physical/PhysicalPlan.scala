package com.octo.mythbuster.spark.sql.plan.physical

import java.net.URL
import java.nio.file.Path
import java.util

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.octo.mythbuster.spark.sql.expression.BinaryOperation
import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.{InternalRow => JavaInternalRow}

import com.octo.mythbuster.spark.Implicits._
import scala.util.{Failure, Success}
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.plan.Plan
import com.octo.mythbuster.spark.sql.plan.physical.codegen.JavaCodeGenerationSupport
import com.octo.mythbuster.spark.sql.{expression => e}
import com.octo.mythbuster.spark.sql.plan.physical.{codegen => c}
import com.octo.mythbuster.spark.{tree => t}
import com.octo.mythbuster.spark.compiler.JavaCode
import com.octo.mythbuster.spark.sql.plan.physical.codegen.Implicits._
import scala.collection.JavaConverters._


// A PhysicalPlan modelize each stage which will be executed during the execution of the query
trait PhysicalPlan extends Plan[PhysicalPlan] with t.TreeNode[PhysicalPlan] {

  def execute(): Iterator[InternalRow]

  def explain(indent: Int = 0): String

  def supportCodeGeneration(): Boolean = {
    this.isInstanceOf[JavaCodeGenerationSupport]
  }

}

// This physical plan parse a CSV in order to produce internal rows
case class CSVFileFullScan(qualifierName: QualifierName, csvFileURL: URL) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  import scala.collection.JavaConverters._

  val Separator = ";"

  override def execute(): Iterator[InternalRow] = {
    val charSource = scala.io.Source.fromURL(csvFileURL)
    val charSourceSeq = charSource.getLines().onEnd({ charSource.close() })//.toSeq // FIXME: Close InputStream on last item
    //inputStream.close()

    var charSourceIterator = charSourceSeq//.iterator
    //val charSource = Resources.asCharSource(csvFileURL, Charsets.UTF_8)
    val columnNames = charSourceIterator.next().split(Separator).map(_.toLowerCase)
    charSourceIterator
      .map({ line =>
        columnNames.zip(line.split(Separator)).toMap[String, Any]
      })
      .map(_.toInternalRow(qualifierName))
  }

  override def explain(indent: Int = 0): String = {
    s"${"  " * indent}CSVFileFullScan(${csvFileURL}) ${if (supportCodeGeneration()) "*" else ""}"
  }

}

// This will filter internal rows which does not fulfill the expression (which should be a predicate)
case class Filter(child: PhysicalPlan, expression: e.Expression) extends PhysicalPlan with JavaCodeGenerationSupport with t.UnaryTreeNode[PhysicalPlan] {

  override def execute() : Iterator[InternalRow] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}Filter(${expression}) ${if (supportCodeGeneration()) "*" else ""}
       |${child.explain(indent + 1)}""".stripMargin
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, variableName: JavaCode): JavaCode = {
    s"""
       |// FILTER
       |if(!(${expression.generateJavaCode(variableName)})) continue;
       |${consumeJavaCode(codeGenerationContext, variableName)}
      """.stripMargin
  }

}

// The CartesianProduct will do a cartesian product of the two child (by keeping in memory the right one)
case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] {

  override def execute(): Iterator[InternalRow] = {
    val rightRows = rightChild.execute().toSeq
    for {
      leftRow <- leftChild.execute()
      rightRow <- rightRows
    } yield leftRow ++ rightRow
  }

  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}CartesianProduct ${if (supportCodeGeneration()) "*" else ""}
       |${leftChild.explain(indent + 1)}
       |${rightChild.explain(indent + 1)}""".stripMargin
  }

}

// The CartesianProduct will do a cartesian product of the two child (by keeping in memory the right one)
case class Join(leftChild: PhysicalPlan, rightChild: PhysicalPlan, operation : BinaryOperation) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] with JavaCodeGenerationSupport {

  override val child = leftChild

  override def execute(): Iterator[InternalRow] = {
    val rightHashMap = LazyIteratorToJavaMap(rightChild.execute(), row => operation.rightChild.evaluate(row))
    leftChild
      .execute()
      .flatMap{ row =>
        val matchedRowOption = Option(rightHashMap.get(operation.leftChild.evaluate(row)))
        matchedRowOption.map(mrow => mrow.unwrapForScala() ++ row)
      }
  }


  override def explain(indent: Int = 0): String = {
    s"""${"  " * indent}CartesianProduct ${if (supportCodeGeneration()) "*" else ""}
       |${leftChild.explain(indent + 1)}
       |${rightChild.explain(indent + 1)}""".stripMargin
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, variableName: JavaCode): JavaCode = {
    val rightJoinMap = LazyIteratorToJavaMap(rightChild.execute(), row => operation.rightChild.evaluate(row))
    val hashMapInputRows = codeGenerationContext.addReference(rightJoinMap, "LazyIteratorToJavaMap<String>")

    s"""
        |// JOIN
        |InternalRow matchedRow = $hashMapInputRows.get(${operation.leftChild.generateJavaCode(variableName)});
        |if(matchedRow == null) continue;
        |$variableName.concatenate(matchedRow);
        |${consumeJavaCode(codeGenerationContext, variableName)}
        |""".stripMargin
  }

}

case class LazyIteratorToJavaMap[KeyType](it : scala.collection.Iterator[InternalRow], keyBuilder : InternalRow => KeyType) {
  lazy val evaluatedMap = it
      .toSeq
      .map(row => keyBuilder(row) -> row.wrapForJava)
      .toMap[KeyType, JavaInternalRow]
      .asJava

  def get(key : Any) = evaluatedMap.get(key.asInstanceOf[KeyType])
}

// The projection will map the InternalRows by applying the expressions
case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with t.UnaryTreeNode[PhysicalPlan] with JavaCodeGenerationSupport {

  def execute(): Iterator[InternalRow] = child.execute().map({ internalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value: Any = expression.evaluate(internalRow)
      (expression match {
        case e.NamedExpression(name) => (None, name)
        case _ => (None, s"column_${index}")
      }) -> value
    }): _*)
  })

  def explain(indent: Int = 0): String = {
    s"""${"  " * indent}Projection(${expressions.mkString(", ")}) ${if (supportCodeGeneration()) "*" else ""}
       |${child.explain(indent + 1)}""".stripMargin
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, variableName: JavaCode): JavaCode = {
    val internalRowWithProjection = codeGenerationContext.freshVariableName()
    val arrayVariableName = codeGenerationContext.freshVariableName()
    codeGenerationContext.addClassAttribute(
      name = arrayVariableName,
      typeName = "String[]",
      initCode = s"new String[] {${expressions.map(_.generateJavaCode(null)).mkString(",")}}"
    )

    val projections = expressions
      .map(_.generateJavaCode(null))

    val projectionsCode = projections
      .map(c => s"${internalRowWithProjection}.setValue($c, $variableName.getValue($c));")
      .mkString("\n")


    s"""
      |// PROJECTION over ${projections.mkString(",")}
      |InternalRow ${internalRowWithProjection} = InternalRow.create();
      |$projectionsCode
      |currentRows.add(${internalRowWithProjection});
    """.stripMargin
  }

}
