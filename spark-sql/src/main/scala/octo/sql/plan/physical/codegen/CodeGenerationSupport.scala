package octo.sql.plan.physical.codegen

import octo.sql.plan.{ physical => p }

trait CodeGenerationSupport {

  val child: p.PhysicalPlan

  protected var parent: CodeGenerationSupport = null

  def produceCode(codeGenerationContext: CodeGenerationContext, parent: CodeGenerationSupport): Code = {
    this.parent = parent
    doProduceCode(codeGenerationContext)
  }

  protected def doProduceCode(codeGenerationContext: CodeGenerationContext): Code = {
    child.asInstanceOf[CodeGenerationSupport].produceCode(codeGenerationContext, this)
  }

  def consumeCode(codeGenerationContext: CodeGenerationContext, rowVariableName: Code): Code = {
    parent.doConsumeCode(codeGenerationContext, rowVariableName)
  }

  protected def doConsumeCode(codeGenerationContext: CodeGenerationContext, rowVariableName: Code): Code = {
    throw new UnsupportedOperationException("This method is not implemented! ")
  }

  def inputRowIterators: Seq[Iterator[p.InternalRow]] = {
    child.asInstanceOf[CodeGenerationSupport].inputRowIterators
  }


}
