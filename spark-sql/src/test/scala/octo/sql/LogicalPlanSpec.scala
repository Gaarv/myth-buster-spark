package octo.sql

import octo.UnitSpec
import octo.sql.lexer.Lexer
import octo.sql.parser.Parser
import octo.sql.plan.logical.LogicalPlan

import scala.util.Success

/*class LogicalPlanSpec extends UnitSpec {

  "The complex query" should "produce a valid logical plan" in {
    val logicalPlan = for {
      tokens <- Lexer(complexQuery)
      ast <- Parser(tokens)
      _ = println(ast)
      logicalPlan <- LogicalPlan(ast)
    } yield logicalPlan

    println(logicalPlan)

    logicalPlan shouldBe a[Success[LogicalPlan]]
  }

}*/
