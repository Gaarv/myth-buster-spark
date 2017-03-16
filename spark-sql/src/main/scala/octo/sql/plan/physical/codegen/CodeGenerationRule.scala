package octo.sql.plan.physical.codegen

import octo.sql.plan.Rule
import octo.sql.plan.physical.PhysicalPlan

object CodeGenerationRule extends Rule[PhysicalPlan] {

  override def apply(physicalPlan: PhysicalPlan) = insertCodeGeneration(physicalPlan)

  protected def insertCodeGeneration(physicalPlan: PhysicalPlan): PhysicalPlan = {
    physicalPlan match {
      case codeGenerationSupport: CodeGenerationSupport => CodeGeneration(insertInput(codeGenerationSupport))
      case _ => physicalPlan.mapChildren(insertCodeGeneration)
    }
  }

  protected def insertInput(physicalPlan: PhysicalPlan): PhysicalPlan = physicalPlan match {
    case codeGenerationSupport: CodeGenerationSupport => codeGenerationSupport.mapChildren(insertInput)
    case _ => Input(insertCodeGeneration(physicalPlan))

  }

}
