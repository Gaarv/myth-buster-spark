package com.octo.mythbuster.spark.sql.plan.physical.codegen

import com.octo.mythbuster.spark.sql.expression.Expression
import com.octo.mythbuster.spark.sql.plan.{physical => p}
import com.octo.mythbuster.spark.{tree => t}

case class Input(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with JavaCodeGenerationSupport {

  override def execute() = child.execute()

  override protected def doProduceJavaCode(codeGenerationContext: JavaCodeGenerationContext) = {
    val variableName = codeGenerationContext.freshVariableName()
    s"""
      |while (hasNextChildRow()) {
      |  InternalRow ${variableName} = nextChildRow();
      |  ${consumeJavaCode(codeGenerationContext, variableName)}
      |
      |  if (!shouldContinue()) return;
      |}
     """.stripMargin
  }

  override def inputRows = child.execute()

  override def produce: Seq[Expression] = child.produce
}
