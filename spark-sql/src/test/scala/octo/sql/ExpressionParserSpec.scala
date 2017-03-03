package octo.sql

import octo.UnitSpec
import octo.sql.{expression => e, lexer => l}
import octo.sql.parser.{ AST, Parser }
import octo.sql.lexer.Lexer

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
