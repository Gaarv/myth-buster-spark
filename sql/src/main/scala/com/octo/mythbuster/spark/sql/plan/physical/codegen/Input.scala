package com.octo.mythbuster.spark.sql.plan.physical.codegen

import com.octo.mythbuster.spark.sql.plan.{ physical => p }
import com.octo.mythbuster.spark.{ tree => t }

case class Input(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with CodeGenerationSupport {

  override def execute() = child.execute()

  override protected def doProduceCode(codeGenerationContext: CodeGenerationContext) = {
    val variableName = codeGenerationContext.freshVariableName()
    s"""
      |while (hasNextChildRow()) {
      |  InternalRow ${variableName} = nextChildRow();
      |  ${consumeCode(codeGenerationContext, variableName)}
      |  if (!shouldContinue()) return;
      |}
     """.stripMargin
  }

  override def explain(indent: Int = 0): String = {
    child.explain(indent)
  }

  override def inputRows = child.execute()

  override def internalFields = child.internalFields

}
