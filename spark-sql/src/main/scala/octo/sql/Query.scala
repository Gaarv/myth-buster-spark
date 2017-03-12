package octo.sql

import octo.sql.lexer.Lexer
import octo.sql.parser.Parser
import octo.sql.plan.QueryPlanner
import octo.sql.plan.logical.LogicalPlan
import octo.sql.plan.physical.PhysicalPlan
import octo.sql.plan.physical._

import scala.util.{Failure, Success, Try}

object Query {

  def apply(sql: String)(implicit tableRegistry: TableRegistry): Try[Query] = for {
    tokens <- Lexer(sql)
    ast <- Parser(tokens)
    logicalPlan <- LogicalPlan(ast)
    physicalPlan <- Success(QueryPlanner.planQuery(logicalPlan))
    _ = println(physicalPlan)
    query <- Success(new Query(physicalPlan, tableRegistry))
  } yield query

}

class Query(physicalPlan: PhysicalPlan, tableRegistry: TableRegistry) {

  def fetch(): Iterator[Row] = physicalPlan.execute().map(_.toRow)

}
