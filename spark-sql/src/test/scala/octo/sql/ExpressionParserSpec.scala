package octo.sql

import scala.util.{ Try, Success, Failure }

class ExpressionParserSpec extends UnitSpec {

  "The expression parser" should "parse successfully" in {

    val parsedAST = (for {
      tokens <- Lexer("(t1.c1 = t2.c2)")
      _ = println(tokens)
      ast <- Parser(tokens)
    } yield ast)

    val expectedAST = e.Equal(e.TableColumn("t1", "c1"), e.TableColumn("t2", "c2"))

    parsedAST should be (Success(expectedAST))

  }

}
