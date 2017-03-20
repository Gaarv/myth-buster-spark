package octo.sql

import scala.util.Success

/*class QueryPlannerSpec extends UnitSpec {

  "The query planner" should "work properly" in {

    implicit val tableRegistry: TableRegistry = Map("t1" -> Seq(), "t2" -> Seq(), "t3" -> Seq())

    val physicalPlan = for {
      tokens <- Lexer(complexQuery)
      ast <- Parser(tokens)
      _ = println(ast)
      logicalPlan <- LogicalPlan(ast)
      physicalPlan <- QueryPlanner.planQuery(logicalPlan)
    } yield physicalPlan

    println(physicalPlan)

    physicalPlan shouldBe a[Success[PhysicalPlan]]

    physicalPlan.map(_.execute()).foreach(_.foreach(println))

  }


}*/
