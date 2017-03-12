package octo.sql.plan

import octo.sql.plan.physical.{PhysicalPlan}
import octo.sql.plan.physical.codegen.CodeGenerator

object QueryOptimizer {

  def optimizeQuery(physicalPlan: PhysicalPlan): PhysicalPlan = {

    null
  }

  protected def optimizeStage() = {
    /*case codeGenerator: CodeGenerator => {
      val code = codeGenerator.generateCode()

    }*/
    null
  }

}
