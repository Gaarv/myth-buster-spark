package com.octo.mythbuster.spark.sql.plan.physical

import java.net.URL
import java.nio.file.{Path, Paths}
import java.util

import com.google.common.base.Charsets
import com.google.common.io.Resources
import com.octo.mythbuster.spark.sql.expression.{BinaryOperation, Expression, TableColumn}
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
import com.octo.mythbuster.spark.sql.plan.logical.TableScan
import com.octo.mythbuster.spark.sql.plan.physical.codegen.Implicits._

import scala.collection.JavaConverters._

// A PhysicalPlan modelize each stage which will be executed during the execution of the query
trait PhysicalPlan extends Plan[PhysicalPlan] with t.TreeNode[PhysicalPlan] {

  def execute(): Iterator[Row]

  override def titleForExplain: String = {
    s"${super.titleForExplain}${if (supportCodeGeneration()) " *" else ""}"
  }

  def supportCodeGeneration(): Boolean = {
    this.isInstanceOf[JavaCodeGenerationSupport]
  }

}

// This physical plan parse a CSV in order to produce internal rows
case class CSVFileFullScan(csvFileURL: URL) extends PhysicalPlan with t.LeafTreeNode[PhysicalPlan] {

  import scala.collection.JavaConverters._

  val Separator = ";"

  override def execute(): Iterator[Row] = {
    val charSource = scala.io.Source.fromURL(csvFileURL)
    val charSourceSeq = charSource.getLines()//.toSeq // FIXME: Close InputStream on last item
    //inputStream.close()

    var charSourceIterator = charSourceSeq//.iterator
    //val charSource = Resources.asCharSource(csvFileURL, Charsets.UTF_8)
    val columnNames = charSourceIterator.next().split(Separator)//.map(_.toLowerCase)
    charSourceIterator
      .map({ line =>
        columnNames.zip(line.split(Separator)).toMap[String, Any]
      })
  }

  override def titleForExplain: String = {
    s"CSVFileFullScan(${Paths.get(csvFileURL.getFile).getFileName})"
  }

  override def produce: Seq[Expression] = {
    val charSource = scala.io.Source.fromURL(csvFileURL)
    val charSourceSeq = charSource.getLines()//.toSeq // FIXME: Close InputStream on last item
    //inputStream.close()

    var charSourceIterator = charSourceSeq//.iterator
    //val charSource = Resources.asCharSource(csvFileURL, Charsets.UTF_8)
    val columnNames = charSourceIterator.next().split(Separator)//.map(_.toLowerCase)

    columnNames.map({ columnName =>
      TableColumn(columnName)
    })
  }

}

// This will filter internal rows which does not fulfill the expression (which should be a predicate)
case class Filter(child: PhysicalPlan, expression: e.Expression) extends PhysicalPlan with JavaCodeGenerationSupport with t.UnaryTreeNode[PhysicalPlan] {

  override def execute() : Iterator[Row] = {
    expression.toPredicate match {
      case Success(predicate) => child.execute().filter(predicate.evaluate)
      case Failure(e) => throw e
    }
  }

  override def titleForExplain: String = {
    s"Filter(${expression})"
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, rowVariableName: JavaCode): JavaCode = {
    s"""
       |if(!(${expression.generateJavaCode(rowVariableName)})) continue;
       |${consumeJavaCode(codeGenerationContext, rowVariableName)}
      """.stripMargin
  }

  override def produce: Seq[Expression] = child.produce

}

case class NestedLoopJoin(leftChild: PhysicalPlan, rightChild: PhysicalPlan, filter: e.BinaryOperation) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] {

  override def execute(): Iterator[Row] = {
    // We do some kind of lazy cartesian product
    val cartesianProduct = for {
      leftRow <- leftChild.execute()
      rightRow <- rightChild.execute()
    } yield leftRow ++ rightRow

    // And we filter the row fitting the filter
    cartesianProduct collect {
      case row: Row if filter.evaluate(row).asInstanceOf[Boolean] => row
    }
  }

  override def titleForExplain: String = {
    s"NestedLoopJoin(${filter})"
  }

  override def produce: Seq[Expression] = leftChild.produce ++ rightChild.produce

}

// This HashJoin will only works if the need column by the leftExpression are provided by the leftChild
case class HashJoin(leftChild: PhysicalPlan, rightChild: PhysicalPlan, filter: e.BinaryOperation) extends PhysicalPlan with t.BinaryTreeNode[PhysicalPlan] with JavaCodeGenerationSupport {

  type Index[Type] = Map[Type, Seq[Row]]

  val child = rightChild

  override def execute(): Iterator[Row] = {
    filter match {
      case e.Equal(leftExpression: e.Expression, rightExpression: e.Expression) =>
        // We index the left part of the physical plan by the filter
        val leftChildIndex = index[rightExpression.Type](leftChild, leftExpression)

        // We loop over the right part and we lookup the indexed right part of the physical plan
        for {
          rightRow <- rightChild.execute()
          leftRow <- leftChildIndex.get(rightExpression.evaluate(rightRow)).getOrElse(Seq())
        } yield leftRow ++ rightRow

      // It's actually a simple case: we only need to reverse the left and right predicates
      case _ => throw new PhysicalPlanException("That's not possible, dude... The filter is too complex, I guess")
    }

  }

  private def index[Type](physicalPlan: PhysicalPlan, expression: e.Expression): Index[Type] = {
    physicalPlan.execute()
        .toSeq
        .groupBy(internalRow => {
          expression.evaluate(internalRow)
        })
        .map({ case (k, v) => (k.asInstanceOf[Type], v) }) // Some checks need to be done, here!
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, rightRowVariableName: JavaCode): JavaCode = {
    filter match {
      case e.Equal(leftExpression: e.Expression, rightExpression: e.Expression) =>
        val leftIndexAsJava = index[rightExpression.Type](leftChild, leftExpression)
            .mapValues(_.map(_.wrapForJava).asJava)
            .asJava

        val leftIndexAsJavaVariableName = codeGenerationContext.addReference(leftIndexAsJava, "Map<Object, List<InternalRow>>")

        val leftRowsVariableName = codeGenerationContext.freshVariableName()
        val leftRowVariableName = codeGenerationContext.freshVariableName()
        val rowVariableName = codeGenerationContext.freshVariableName()

        s"""
           |List<InternalRow> ${leftRowsVariableName} = ${leftIndexAsJavaVariableName}.get(${rightExpression.generateJavaCode(rightRowVariableName)});
           |if(${leftRowsVariableName} == null) continue;
           |for (InternalRow ${leftRowVariableName} : ${leftRowsVariableName}) {
           |  InternalRow ${rowVariableName} = ${rightRowVariableName}.concatenate(${leftRowVariableName});
           |  ${consumeJavaCode(codeGenerationContext, rowVariableName)}
           |}
           |""".stripMargin

      case _ =>
        throw new PhysicalPlanException("That's not possible, dude... The filter is too complex, I guess")
    }

  }

  override def titleForExplain: String = {
    s"HashJoin(${filter})"
  }

  override def produce: Seq[Expression] = leftChild.produce ++ rightChild.produce

}

// The projection will map the InternalRows by applying the expressions
case class Projection(child: PhysicalPlan, expressions : Seq[e.Expression]) extends PhysicalPlan with t.UnaryTreeNode[PhysicalPlan] with JavaCodeGenerationSupport {

  def execute(): Iterator[Row] = child.execute().map({ internalRow =>
    Map(expressions.zipWithIndex.map({ case (expression: e.Expression, index: Int) =>
      val value: Any = expression.evaluate(internalRow)
      (expression match {
        case e.NamedExpression(name) => name
        case _ => s"column_${index}"
      }) -> value
    }): _*)
  })

  override def titleForExplain: String = {
    s"Projection(${expressions.mkString(", ")})"
  }

  override def doConsumeJavaCode(codeGenerationContext: c.JavaCodeGenerationContext, rowVariableName: JavaCode): JavaCode = {
    val newRowVariableName = codeGenerationContext.freshVariableName()

    val projectionCode = expressions.zipWithIndex
      .map({ case (expression, index) =>
        (expression match {
          case e.NamedExpression(name) => (name)
          case _ => (s"column_${index}")
        }) -> expression.generateJavaCode(rowVariableName)
      })
      .map({ case (columnName, valueJavaCode) =>
        s"""${newRowVariableName}.setValue("${columnName}", ${valueJavaCode});"""
      })
      .mkString("\n")

    s"""
      |InternalRow ${newRowVariableName} = InternalRow.create();
      |${projectionCode}
      |
      |${consumeJavaCode(codeGenerationContext, newRowVariableName)}
    """.stripMargin
  }

  override def produce: Seq[Expression] = expressions

}
