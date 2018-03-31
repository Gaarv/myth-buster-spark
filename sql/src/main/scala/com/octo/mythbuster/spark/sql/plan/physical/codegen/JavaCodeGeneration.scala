package com.octo.mythbuster.spark.sql.plan.physical.codegen

import scala.util.{Failure, Success}
import scala.collection.{Iterator => ScalaIterator}
import java.util.{Iterator => JavaIterator}

import com.octo.mythbuster.spark.{Caching, Logging, tree => t}
import com.octo.mythbuster.spark.compiler._
import com.octo.mythbuster.spark.sql.plan.{physical => p}
import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.{CodeGeneratedInternalRowIterator, InternalRow => JavaInternalRow}
import p.{Row => ScalaRow}
import Implicits._
import com.octo.mythbuster.spark.sql.expression.Expression

object JavaCodeGeneration extends Caching[JavaClassSource, ScalaIterator[ScalaRow]]

case class JavaCodeGeneration(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with JavaCodeGenerationSupport with Logging {

  import JavaCodeGeneration._

  def generateJavaCode(codeGenerationContext : JavaCodeGenerationContext): JavaCode = {
    val generatedCode = child.asInstanceOf[JavaCodeGenerationSupport].produceJavaCode(codeGenerationContext, this)
    generatedCode
  }

  def generateClassSource(codeGenerationContext : JavaCodeGenerationContext): JavaClassSource = {
    val methodCode = generateJavaCode(codeGenerationContext)

    val packageName = "octo.sql.physical.codegen.impl"
    val classSimpleName = "CodeGeneratedInternalRowIteratorImpl"
    val className = s"${packageName}.${classSimpleName}"
    val classCode =
      s"""package ${packageName};
         |
         |import java.util.Iterator;
         |import java.util.List;
         |import java.io.IOException;
         |import java.util.Map;
         |import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.CodeGeneratedInternalRowIterator;
         |import com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper.InternalRow;
         |import java.util.LinkedList;
         |import java.util.HashMap;
         |import java.util.Optional;
         |
         |
         |public class $classSimpleName extends CodeGeneratedInternalRowIterator {
         |
         |   ${codeGenerationContext.generateAttributesDeclarationCode}
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
         |   @Override
         |   public void init(Object[] references) {
         |      ${codeGenerationContext.generateInitCode}
         |   }
         |
         |}
      """.stripMargin

    logger.debug("classCode=\n{}", CodeFormatter.format(classCode))

    JavaClassSource(className, classCode)
  }

  def execute(): ScalaIterator[ScalaRow] = {
    println("execute")
    val codeGenerationContext = JavaCodeGenerationContext()
    cache.get(generateClassSource(codeGenerationContext)) { classSource =>
      classSource.compile() match {
        case Success(generatedClass) => newInstanceOfGeneratedClass(generatedClass, child.asInstanceOf[JavaCodeGenerationSupport].inputRows, codeGenerationContext.getReferencesAsArray)
        case Failure(e) => throw e
      }
    }
  }

  override protected def doProduceJavaCode(codeGenerationContext: JavaCodeGenerationContext) = {
    throw new UnsupportedOperationException("This method is not implemented! ")
  }

  override def inputRows = {
    throw new UnsupportedOperationException("This method is not implemented! ")
  }

  override def doConsumeJavaCode(codeGenerationContext: JavaCodeGenerationContext, rowVariableName: JavaCode): JavaCode = {
    s"""
       |addToCurrentRow(${rowVariableName});
     """.stripMargin.trim
  }

  protected def newInstanceOfGeneratedClass(generatedClass: Class[_], childRows: ScalaIterator[ScalaRow], references : Array[Object]): ScalaIterator[ScalaRow] = {
    val constructor = generatedClass.getConstructor(classOf[JavaIterator[JavaInternalRow]])
    val newInstance = constructor.newInstance(childRows.wrapForJava).asInstanceOf[CodeGeneratedInternalRowIterator]
    newInstance.init(references)
    newInstance.unwrapForScala()
  }

  override def produce: Seq[Expression] = child.produce
}
