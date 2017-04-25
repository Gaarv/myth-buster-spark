package com.octo.mythbuster.spark.sql.plan.physical.codegen

import com.octo.mythbuster.spark.sql.plan.Rule
import com.octo.mythbuster.spark.sql.plan.physical.PhysicalPlan

object GenerateJavaCode extends Rule[PhysicalPlan] {

  override def apply(physicalPlan: PhysicalPlan) = insertCodeGeneration(physicalPlan)

  // We wrap every
  protected def insertCodeGeneration(physicalPlan: PhysicalPlan): PhysicalPlan = {
    println("Applying generate java code : " +physicalPlan)
    physicalPlan match {
      case codeGenerationSupport: JavaCodeGenerationSupport => JavaCodeGeneration(insertInput(codeGenerationSupport))
      case _ => physicalPlan.mapChildren(insertCodeGeneration)
    }
  }

  // We wrap the first children which can't support the code generation in order to stop the produceCode() / consumeCode() mecanism
  protected def insertInput(physicalPlan: PhysicalPlan): PhysicalPlan = {
    println("Insert input : " + physicalPlan)
    physicalPlan match {
      case codeGenerationSupport: JavaCodeGenerationSupport => codeGenerationSupport.mapChildren(insertInput)
      case _ => Input(insertCodeGeneration(physicalPlan))

    }
  }

}
