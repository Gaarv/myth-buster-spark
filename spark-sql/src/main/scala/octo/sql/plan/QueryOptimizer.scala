package octo.sql.plan

import octo.sql.plan.physical.{PhysicalPlan, Stage}
import octo.sql.plan.physical.codegen.CodeGenerator

object QueryOptimizer {

  def optimizeQuery(physicalPlan: PhysicalPlan): PhysicalPlan = {

    null
  }

  protected def optimizeStage(stage: Stage) = {
    /*case codeGenerator: CodeGenerator => {
      val code = codeGenerator.generateCode()

    }*/
    null
  }

}
