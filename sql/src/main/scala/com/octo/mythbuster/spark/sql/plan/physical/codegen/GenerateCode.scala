package com.octo.mythbuster.spark.sql.plan.physical.codegen

import com.octo.mythbuster.spark.sql.plan.Rule
import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlan

object GenerateCode extends Rule[PhysicalPlan] {

  override def apply(physicalPlan: PhysicalPlan) = insertCodeGeneration(physicalPlan)

  // We wrap every
  protected def insertCodeGeneration(physicalPlan: PhysicalPlan): PhysicalPlan = {
    physicalPlan match {
      case codeGenerationSupport: CodeGenerationSupport => CodeGeneration(insertInput(codeGenerationSupport))
      case _ => physicalPlan.mapChildren(insertCodeGeneration)
    }
  }

  // We wrap the first children which can't support the code generation in order to stop the produceCode() / consumeCode() mecanism
  protected def insertInput(physicalPlan: PhysicalPlan): PhysicalPlan = physicalPlan match {
    case codeGenerationSupport: CodeGenerationSupport => codeGenerationSupport.mapChildren(insertInput)
    case _ => Input(insertCodeGeneration(physicalPlan))

  }

}
