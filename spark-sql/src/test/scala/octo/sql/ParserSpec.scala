package octo.sql

import octo.UnitSpec
import octo.sql.expression.TableColumn
import octo.sql.lexer.Lexer
import octo.sql.parser._
import octo.sql.expression._

import scala.util.Success

class ParserSpec extends UnitSpec {

  val query = "SELECT t1.c2 FROM t3, t1 JOIN t2 ON t1.c1 = t2.c1 WHERE t1.c2 > t2.c2"

  val queryWithAnd =
    """
      |SELECT
      |  t1.c2
      |FROM
      |  t3,
      |  t1
      |JOIN
      |  t2
      |ON
      |  t1.c1 = t2.c1
      |WHERE
      |  t1.c2 > t2.c2
   """.stripMargin

  "The SQL parser" should "works properly with a valid query" in {
    val parsedAST = (for {
      tokens <- Lexer(queryWithAnd)
      ast <- Parser(tokens)
    } yield ast)

    val expectedAST = Select(
      Seq(
        TableColumn("t1", "c2")
      ),
      Some(
        Greater(
          TableColumn("t1", "c2"),
          TableColumn("t2", "c2")
        )
      ),
      Seq(
        Table("t3"),
        Join(
          Equal(TableColumn("t1", "c1"), TableColumn("t2", "c1")),
          Table("t1"),
          Table("t2")
        )
      )
    )

    parsedAST should be (Success(expectedAST))

  }

}
