package com.octo.mythbuster.spark.sql.plan.physical.codegen

import scala.collection.JavaConverters._
import com.octo.mythbuster.spark.Logging
import com.octo.mythbuster.spark.sql.plan.{physical => p}
import com.octo.mythbuster.spark.{tree => t}
import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlan
import com.octo.mythbuster.spark.sql.plan.physical.codegen.Implicits._
import com.octo.mythbuster.spark.compiler._
import java.util.{Iterator => JavaIterator}

import com.octo.mythbuster.spark.sql.plan.physical.codegen.spi.{ CodeGeneratedInternalRowIterator, InternalRow => JavaInternalRow }
import com.octo.mythbuster.spark.sql.plan.physical.{InternalRow => ScalaInternalRow}

import scala.util.{Failure, Success}

case class CodeGeneration(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with CodeGenerationSupport with Logging {

  def generateCode(): Code = {
    val codeGenerationContext = CodeGenerationContext()
    val generatedCode = child.asInstanceOf[CodeGenerationSupport].produceCode(codeGenerationContext, this)
    generatedCode
  }

  def execute(): Iterator[p.InternalRow] = {
    val code = generateCode()

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
        |import octo.sql.plan.physical.codegen.spi.CodeGeneratedInternalRowIterator;
        |import octo.sql.plan.physical.codegen.spi.InternalRow;
        |import octo.sql.plan.physical.codegen.spi.TableNameAndColumnName;
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
        |     ${code}
        |   }
        |
        |}
      """.stripMargin

    logger.debug("classCode={}", classCode.split("\n").zipWithIndex.map({ case (line, index) =>
      s"${index} ${line}"
    }).mkString("\n"))

    JavaClassSource(className, classCode).compile() match {
      case Success(generatedClass) => newInstanceOfGeneratedClass(generatedClass, child.asInstanceOf[CodeGenerationSupport].inputRows)
      case Failure(e) => throw e
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

  protected def newInstanceOfGeneratedClass(generatedClass: Class[_], childRows: Iterator[ScalaInternalRow]): Iterator[ScalaInternalRow] = {
    val constructor = generatedClass.getConstructor(classOf[JavaIterator[JavaInternalRow]])
    constructor.newInstance(childRows.wrapForJava).asInstanceOf[CodeGeneratedInternalRowIterator].unwrapForScala
  }

}
