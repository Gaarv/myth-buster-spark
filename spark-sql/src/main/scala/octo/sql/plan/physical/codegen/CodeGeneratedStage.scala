package octo.sql.plan.physical.codegen

import octo.sql.plan.{physical => p}
import octo.sql.plan.physical.PhysicalPlan
import octo.sql.plan.physical.codegen.Implicits._
import octo.compiler.JavaClassCompiler.global
import octo.compiler.JavaClassSpec

import java.util.{ Iterator => JavaIterator }
import octo.sql.plan.physical.codegen.{ InternalRow => JavaInternalRow }
import octo.sql.plan.physical.{ InternalRow => ScalaInternalRow }

import scala.util.{Failure, Success}

case class CodeGeneratedStage(childStage: PhysicalPlan, generatedCode: String) extends PhysicalPlan {

  def execute(): Iterator[p.InternalRow] = {
    val packageName = "octo.sql.physical.codegen"
    val generatedClassSimpleName = "InternalRowCodeGeneratedIteratorImpl"
    val generatedClassName = s"${packageName}.${generatedClassSimpleName}"
    val generatedClassSourceCode =
      s"""
        |package ${packageName};
        |
        |import java.util.Iterator;
        |import com.octo.nad.GeneratedIterator;
        |import java.io.IOException;
        |import java.util.Map;
        |import octo.sql.plan.physical.codegen.InternalRowCodeGeneratedIterator;
        |import java.util.LinkedList;
        |import java.util.HashMap;
        |
        |
        |public class $generatedClassSimpleName extends InternalRowCodeGeneratedIterator {
        |
        |   public $generatedClassSimpleName(Iterator<InternalRow> childRows) {
        |     super(childRows);
        |   }
        |
        |   @Override
        |   protected void continueProcessing() {
        |     while(shouldContinueProcessing() && addNextChildRowToCurrentRows()) {
        |        ${generatedCode}
        |      }
        |   }
        |
        |}
      """.stripMargin

      JavaClassSpec(generatedClassName, generatedClassSourceCode).compile() match {
        case Success(generatedClass) => newInstanceOfGeneratedClass(generatedClass, childStage.execute())
        case Failure(e) => throw e
      }
  }

  protected def newInstanceOfGeneratedClass(generatedClass: Class[_], childRows: Iterator[ScalaInternalRow]): Iterator[ScalaInternalRow] = {
    val constructor = generatedClass.getConstructor(classOf[JavaIterator[JavaInternalRow]])
    constructor.newInstance(childRows.wrapForJava).asInstanceOf[InternalRowCodeGeneratedIterator].unwrapForScala
  }

}
