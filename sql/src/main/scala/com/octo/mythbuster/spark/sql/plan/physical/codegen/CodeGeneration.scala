package com.octo.mythbuster.spark.sql.plan.physical.codegen

import scala.util.{Failure, Success}
import scala.collection.{Iterator => ScalaIterator}
import java.util.{Iterator => JavaIterator}

import com.octo.mythbuster.spark.{Caching, Logging, tree => t}
import com.octo.mythbuster.spark.compiler._
import com.octo.mythbuster.spark.sql.plan.{physical => p}
import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.{CodeGeneratedInternalRowIterator, InternalRow => JavaInternalRow}
import com.octo.mythbuster.spark.sql.plan.physical.{InternalRow => ScalaInternalRow}
import Implicits._

case class CodeGeneration(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with CodeGenerationSupport with Logging with Caching[JavaClassSource, Iterator[p.InternalRow]] {

  def generateMethodCode(): Code = {
    val codeGenerationContext = CodeGenerationContext()
    val generatedCode = child.asInstanceOf[CodeGenerationSupport].produceCode(codeGenerationContext, this)
    generatedCode
  }

  def generateClassSource(): JavaClassSource = {
    val methodCode = generateMethodCode()

    val packageName = "octo.sql.physical.codegen.impl"
    val classSimpleName = "CodeGeneratedInternalRowIteratorImpl"
    val className = s"${packageName}.${classSimpleName}"
    val classCode =
      s"""
        |package ${packageName};
        |
        |import java.util.Iterator;
        |import java.io.IOException;
        |import java.util.Map;
        |import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.CodeGeneratedInternalRowIterator;
        |import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.InternalRow;
        |import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.TableNameAndColumnName;
        |import java.util.LinkedList;
        |import java.util.HashMap;
        |import java.util.Optional;
        |
        |
        |public class $classSimpleName extends CodeGeneratedInternalRowIterator {
        |
        |   public $classSimpleName(Iterator<InternalRow> childRows) {
        |     super(childRows);
        |   }
        |
        |   @Override
        |   protected void doContinue() {
        |     ${methodCode}
        |   }
        |
        |}
      """.stripMargin

    logger.debug("classCode={}", classCode.split("\n").zipWithIndex.map({ case (line, index) =>
      s"${index} ${line}"
    }).mkString("\n"))

    JavaClassSource(className, classCode)
  }

  def execute(): Iterator[p.InternalRow] = {
    cache.get(generateClassSource()) { classSource =>
      classSource.compile() match {
        case Success(generatedClass) => newInstanceOfGeneratedClass(generatedClass, child.asInstanceOf[CodeGenerationSupport].inputRows)
        case Failure(e) => throw e
      }
    }
  }

  override protected def doProduceCode(codeGenerationContext: CodeGenerationContext) = {
    throw new UnsupportedOperationException("This method is not implemented! ")
  }

  override def inputRows = {
    throw new UnsupportedOperationException("This method is not implemented! ")
  }

  override def doConsumeCode(codeGenerationContext: CodeGenerationContext, rowVariableName: Code): Code = {
    s"""
       |append(${rowVariableName});
     """.stripMargin.trim
  }

  protected def newInstanceOfGeneratedClass(generatedClass: Class[_], childRows: ScalaIterator[ScalaInternalRow]): ScalaIterator[ScalaInternalRow] = {
    val constructor = generatedClass.getConstructor(classOf[JavaIterator[JavaInternalRow]])
    constructor.newInstance(childRows.wrapForJava).asInstanceOf[CodeGeneratedInternalRowIterator].unwrapForScala
  }

  def explain(indent: Int = 0): String = {
    child.explain(indent)
  }

}
