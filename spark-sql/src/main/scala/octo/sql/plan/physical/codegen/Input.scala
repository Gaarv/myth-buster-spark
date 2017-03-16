package octo.sql.plan.physical.codegen

import octo.sql.plan.{ physical => p }
import octo.{ tree => t }

case class Input(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with CodeGenerationSupport {

  override def execute() = child.execute()

  override protected def doProduceCode(codeGenerationContext: CodeGenerationContext) = {
    val variableName = codeGenerationContext.freshVariableName()
    s""" while (hasNextChildRow()) {
       |   InternalRow ${variableName} = nextChildRow();
       |   ${consumeCode(codeGenerationContext, variableName)}
       |   if (!shouldContinue()) return;
       | }
     """.stripMargin
  }

  override def inputRows = child.execute()

}
