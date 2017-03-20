package octo.sql

import scala.util.{Failure, Success, Try}

object Query {

  def apply(sql: String)(implicit tableRegistry: TableRegistry): Try[Query] = for {
    tokens <- Lexer(sql)
    ast <- Parser(tokens)
    logicalPlan <- LogicalPlan(ast)
    optimizedLogicalPlan = LogicalPlanOptimizer.optimizePlan(logicalPlan)
    physicalPlan <- QueryPlanner.planQuery(optimizedLogicalPlan)
    optimizedPhysicalPlan = PhysicalPlanOptimizer.optimizePlan(physicalPlan)
    _ = println(optimizedPhysicalPlan)
  } yield new Query(optimizedPhysicalPlan, tableRegistry)

}

class Query(physicalPlan: PhysicalPlan, tableRegistry: TableRegistry) {

  def fetch(): Iterator[Row] = physicalPlan.execute().map(_.toRow)

}
