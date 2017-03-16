package octo.sql.plan.physical.codegen

import octo.sql.plan.{ physical => p }
import octo.{ tree => t }

case class Input(child: p.PhysicalPlan) extends p.PhysicalPlan with t.UnaryTreeNode[p.PhysicalPlan] with CodeGenerationSupport {

  override def execute() = child.execute()

  override protected def doProduceCode(codeGenerationContext: CodeGenerationContext) = {
    val input = "toto"
    var row = "tata"
    s"""
       | while ($input.hasNext() && !stopEarly()) {
       |   InternalRow $row = (InternalRow) $input.next();
       |   ${consumeCode(codeGenerationContext, row).trim}
       |   if (shouldStop()) return;
       | }
     """.stripMargin
  }

  override def inputRowIterators = child.execute() :: Nil

}
