package com.octo.mythbuster.spark.sql.catalyst.plans.physical

import java.util

import com.octo.mythbuster.spark.sql.catalyst.expressions.codegen.{JavaClassSpec, JavacJavaClassCompiler, JavaClassCompiler}
import com.octo.mythbuster.spark.sql.catalyst.parser.TableColumn
import com.octo.mythbuster.spark.sql.catalyst.plans.logical
import com.octo.mythbuster.spark.sql.catalyst.plans.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.{ Row, TableName }
import com.octo.mythbuster.spark.sql.catalyst.expressions.Predicate

import java.util.{ Iterator => JavaIterator, Map => JavaMap }
import com.octo.nad.GeneratedIterator

import scala.collection.JavaConverters._

sealed trait PhysicalPlan {

  def explain(indentCount: Int = 0): String

  def execute(withCodeGeneration: Boolean = false): Iterator[Row] = {
    executeVolcano()
  }

  def executeVolcano(): Iterator[Row]

}

trait CodeGen extends PhysicalPlan {
  def generateCode(parentCode : String): String

  override def execute(withCodeGeneration: Boolean): Iterator[Row] = {
    if(withCodeGeneration) executeGenerated()
    else executeVolcano()
  }

  def executeGenerated(): Iterator[Row] = {
    val packageName = "com.octo.nad"
    val generatedClassSimpleName = "GeneratedObject"
    val generatedClassName = packageName + "." + generatedClassSimpleName
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
        |     ${generateCode("")}
        |   }
        |
        |}
      """.stripMargin

    println("### CODE : \n" + code )

    val generatedIterator : GeneratedIterator = JavacJavaClassCompiler.compile(JavaClassSpec(generatedClassName, code)).get.newInstance().asInstanceOf[GeneratedIterator]
    val next = generatedIterator.next()
    next.asInstanceOf[scala.collection.Iterator[Row]]
  }
}

case class IterableScan(iterable: Iterable[Row]) extends CodeGen {

  override def generateCode(parentCode: String): String = {
    s"""
      while(currentRows.hasNext()) {
        $parentCode
      }
     """
  }

  override def explain(indentCount: Int): String = {
    val indent = "\t" * indentCount
    s"${indent}IterableScan(iterable=[\n${"\t" * (indentCount + 1)}${iterable.map({ row => s"(${row.values.mkString(", ")})" }).mkString(", ")}\n${indent}])"
  }

  override def executeVolcano(): Iterator[Row] = {
    iterable.iterator
  }
}

case class Filter(child: PhysicalPlan, predicate: Predicate) extends CodeGen {

  override def executeVolcano() : Iterator[Row] = {
    child.executeVolcano().filter(predicate.evaluate)
  }

  override def generateCode(parentCode: String): String = {
    val code : String =
      s"""
         |Map<String, Object> currentRow = currentRows.getFirst();
         if(${predicate.toString}) {
         |   $parentCode
         }
       """.stripMargin
    child match {
      case t : CodeGen  => t.generateCode(code)
      case _ => code
    }
  }

  override def explain(indentCount: Int): String = {
    val indent = "\t" * indentCount
    s"${indent}Filter(predicate=${predicate}), \n${child.explain(indentCount + 1)}\n${indent})"
  }
}

case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends PhysicalPlan {

  override def explain(indentCount: Int = 0): String = {
    val indent = "\t" * indentCount
    s"${indent}CartesianProduct(\n${leftChild.explain(indentCount + 1)},\n${rightChild.explain(indentCount + 1)}\n${indent})"
  }

  override def executeVolcano(): Iterator[Row] = {
    val leftChildIterator = leftChild.execute()
    val rightChildSeq = rightChild.execute().toSeq
    for {
      leftRow <- leftChildIterator
      rightRow <- rightChildSeq
    } yield leftRow ++ rightRow
  }
}

case class Projection(child : PhysicalPlan, tableColumns : Seq[TableColumn]) extends CodeGen {
  override def explain(indentCount: Int): String = {
    val indent = "\t" * indentCount
    s"${indent}Projection(predicate=${tableColumns.mkString(", ")}), \n${child.explain(indentCount + 1)}\n${indent})"
  }

  override def executeVolcano(): Iterator[Row] = {
    child.execute().map(_.filter{
      case (tableColumn, _) => tableColumns.contains(tableColumn)
    })
  }

  override def generateCode(parentCode: String): String = {
    val code = "String tableColumns[] = {" + tableColumns.mkString("\"", "\",\"", "\"") + "};" +
      """
      |Map<String, Object> currentRow = currentRows.getFirst();
      |Map<String, Object> newRow = new HashMap<>();
      |for(String tableColumn : tableColumns) {
      |   newRow.put(tableColumn, currentRow.get(tableColumn));
      |}
      |currentRows.pop();
      |currentRows.add(newRow);
    """.stripMargin + parentCode
    child match {
      case t : CodeGen => t.generateCode(code)
      case _ => code
    }
  }
}

object PhysicalPlan {

  def apply(logicalPlan: LogicalPlan)(implicit iterablesByTableName: Map[TableName, Iterable[Row]]): PhysicalPlan = logicalPlan match {
    case logical.CartesianProduct(leftChild, rightChild) => CartesianProduct(apply(leftChild), apply(rightChild))
    case logical.Filter(child, predicate) => Filter(apply(child), predicate)
    case logical.Scan(tableName: TableName) => IterableScan(iterablesByTableName(tableName))
    case logical.Projection(child, tableColumns : Seq[TableColumn]) => Projection(PhysicalPlan(child), tableColumns)
  }

}
