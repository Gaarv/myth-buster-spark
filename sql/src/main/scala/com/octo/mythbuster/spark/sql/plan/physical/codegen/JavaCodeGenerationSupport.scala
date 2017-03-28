package com.octo.mythbuster.spark.sql.plan.physical.codegen

import com.octo.mythbuster.spark.sql.plan.{ physical => p }
import com.octo.mythbuster.spark.compiler.JavaCode

trait JavaCodeGenerationSupport {

  val child: p.PhysicalPlan

  protected var parent: JavaCodeGenerationSupport = null

  def produceJavaCode(codeGenerationContext: JavaCodeGenerationContext, parent: JavaCodeGenerationSupport): JavaCode = {
    this.parent = parent
    doProduceJavaCode(codeGenerationContext)
  }

  protected def doProduceJavaCode(codeGenerationContext: JavaCodeGenerationContext): JavaCode = {
    child.asInstanceOf[JavaCodeGenerationSupport].produceJavaCode(codeGenerationContext, this)
  }

  def consumeJavaCode(codeGenerationContext: JavaCodeGenerationContext, rowVariableName: JavaCode): JavaCode = {
    parent.doConsumeJavaCode(codeGenerationContext, rowVariableName)
  }

  protected def doConsumeJavaCode(codeGenerationContext: JavaCodeGenerationContext, rowVariableName: JavaCode): JavaCode = {
    throw new UnsupportedOperationException("This method is not implemented! ")
  }

  def inputRows: Iterator[p.InternalRow] = {
    child.asInstanceOf[JavaCodeGenerationSupport].inputRows
  }


}
