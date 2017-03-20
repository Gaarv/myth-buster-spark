package octo.sql

import scala.util.{Failure, Success, Try}

class SQLSpec extends UnitSpec {

  val validQueries = Map(
    "SELECT a.c1 FROM c" -> Seq(Select(), Identifier("a"), Dot(), Identifier("c1"), From(), Identifier("c")),
    "SELECT 0.4 FROM c" -> Seq(Select(), Number(0.4f), From(), Identifier("c"))
  )

  for ((validQuery, tokens) <- validQueries) {
    "The lexer" should s"success with ${validQuery}" in {
      Lexer(validQuery) should be (Success(tokens))
    }
  }

  val invalidQueries = Seq(
    "www.GOOGLE.fr",
    ""
  )

  for (invalidQuery <- invalidQueries) {
    "The lexer" should s"fail while lexing ${invalidQuery}" in {
      Lexer(invalidQuery) shouldBe an[Failure[_]]
    }
  }

}
