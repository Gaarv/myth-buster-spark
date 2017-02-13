package com.octo.mythbuster.spark.sql.catalyst.plans.physical

import com.octo.mythbuster.spark.sql.catalyst.expressions.codegen.{JavaClassSpec, JavacJavaClassCompiler, JavaClassCompiler}
import com.octo.mythbuster.spark.sql.catalyst.plans.logical
import com.octo.mythbuster.spark.sql.catalyst.plans.logical.LogicalPlan
import com.octo.mythbuster.spark.sql.{ Row, TableName }
import com.octo.mythbuster.spark.sql.catalyst.expressions.Predicate

import java.util.{ Iterator => JavaIterator, Map => JavaMap }
import scala.collection.JavaConverters._

sealed trait PhysicalPlan {

  def explain(indentCount: Int = 0): String

  def execute(withCodeGeneration: Boolean): Iterator[Row] = {
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
    val generatedClassName = "GeneratedIterator"
    val code =
      s"""
        |import java.util.Iterator
        |
        |public class ${generatedClassName} implements Iterator<Map<String, Object>> {
        |
        |   public
        |
        |}
      """.stripMargin

    JavacJavaClassCompiler.compile(JavaClassSpec(generatedClassName, code)).get.newInstance().asInstanceOf[JavaIterator[JavaMap[String, Object]]].asScala.map(_.asScala)
  }
}

case class IterableScan(iterable: Iterable[Row]) extends CodeGen {

  override def generateCode(parentCode: String): String = {
    s"""
      while(...) $parentCode
     """
  }

  override def explain(indentCount: Int): String = {
    val indent = "\t" * indentCount
    s"${indent}IterableScan(iterable=[\n${"\t" * (indentCount + 1)}${iterable.map({ row => s"(${row.values.mkString(", ")})" }).mkString(", ")}\n${indent}])"
  }

  override def execute(): Iterator[Row] = ???
}

case class Filter(child: PhysicalPlan, predicate: Predicate) extends CodeGen {
  override def generateCode(parentCode: String): String = {
    child match {
      case t : CodeGen  => val code = s"""
        if($predicate) {
          $parentCode
        }
        """
        child.generateCodeNext(code)
      case _ => ""
    }

  }

  override def explain(indentCount: Int): String = {
    val indent = "\t" * indentCount
    s"${indent}Filter(predicate=${predicate}), \n${child.explain(indentCount + 1)}\n${indent})"
  }
}

case class CartesianProduct(leftChild: PhysicalPlan, rightChild: PhysicalPlan) extends CodeGen {

  override def generateCode(parent : PhysicalPlan) : String = {
    ""
  }

  override def explain(indentCount: Int = 0): String = {
    val indent = "\t" * indentCount
    s"${indent}CartesianProduct(\n${leftChild.explain(indentCount + 1)},\n${rightChild.explain(indentCount + 1)}\n${indent})"
  }

  override def execute(): Iterator[Row] = {
    val leftChildIterator = leftChild.execute()
    val rightChildSeq = rightChild.execute().toSeq
    for {
      leftRow <- leftChildIterator
      rightRow <- rightChildSeq
    } yield leftRow ++ rightRow
  }
}

object PhysicalPlan {

  def apply(logicalPlan: LogicalPlan)(implicit iterablesByTableName: Map[TableName, Iterable[Row]]): PhysicalPlan = logicalPlan match {
    case logical.CartesianProduct(leftChild, rightChild) => CartesianProduct(apply(leftChild), apply(rightChild))
    case logical.Filter(child, predicate) => Filter(apply(child), predicate)
    case logical.Scan(tableName: TableName) => IterableScan(iterablesByTableName(tableName))
  }

}
