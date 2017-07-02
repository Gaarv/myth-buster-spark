package com.octo.mythbuster.spark.sql.lexer

import com.octo.mythbuster.spark.UnitSpec
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

/**
  * Created by marc on 02/07/2017.
  */
class LexerSpec extends WordSpec with Matchers {

  "Lexer" should {
    "recognize AND token" in {
      Lexer("  AND  ") should be (Success(Seq(And())))
    }

    "recognize AS token " in {
      Lexer("  AS  ") should be (Success(Seq(As())))
    }

    "recognize , token " in {
      Lexer("  ,  ") should be (Success(Seq(Comma())))
    }

    "recognize . token " in {
      Lexer("  .  ") should be (Success(Seq(Dot())))
    }

    "recognize = token " in {
      Lexer("  =  ") should be (Success(Seq(Equal())))
    }

    "recognize FALSE token " in {
      Lexer("  FALSE  ") should be (Success(Seq(False())))
    }

    "recognize FROM token " in {
      Lexer("  FROM  ") should be (Success(Seq(From())))
    }

    "recognize > token " in {
      Lexer("  >  ") should be (Success(Seq(Greater())))
    }

    "recognize Identifier token " in {
      Lexer("  hello  ") should be (Success(Seq(Identifier("hello"))))
    }

    "recognize JOIN token " in {
      Lexer("  JOIN  ") should be (Success(Seq(Join())))
    }

    "recognize ( token " in {
      Lexer("  (  ") should be (Success(Seq(LeftParenthesis())))
    }

    "recognize < token " in {
      Lexer("  <  ") should be (Success(Seq(Less())))
    }

    "recognize Number token " in {
      Lexer("  -04.00810  ") should be (Success(Seq(Number(-4.0081f))))
    }

    "recognize ON token " in {
      Lexer("  ON  ") should be (Success(Seq(On())))
    }

    "recognize OR token " in {
      Lexer("  OR  ") should be (Success(Seq(Or())))
    }

    "recognize ) token " in {
      Lexer("  )  ") should be (Success(Seq(RightParenthesis())))
    }

    "recognize SELECT token " in {
      Lexer("  SELECT  ") should be (Success(Seq(Select())))
    }

    "recognize * token " in {
      Lexer("  *  ") should be (Success(Seq(Star())))
    }

    "recognize Text token " in {
      Lexer("  'hello'  ") should be (Success(Seq(Text("hello"))))
    }

    "recognize TRUE token " in {
      Lexer("  TRUE  ") should be (Success(Seq(True())))
    }

    "recognize WHERE token " in {
      Lexer("  WHERE  ") should be (Success(Seq(Where())))
    }

    "be able to recognize all tokens" in {
      val sqlRequest =
        """
          |SELECT
          |   *, table1.column1
          |FROM
          |   table1 t1
          |JOIN
          |   table2 t2
          |ON
          |   t1.column2 = t2.column3
          |WHERE
          |   t2.column1 = 'value'
          |   AND (
          |     t1.column4 > 3.1
          |     OR t1.column4 < -3.1 )
          |   AND t2.column5 = TRUE
          |   AND t2.column6 = FALSE
        """.stripMargin

      val expectedTokens = Seq(
        Select(),
        Star(), Comma(), Identifier("table1"), Dot(), Identifier("column1"),
        From(),
        Identifier("table1"), Identifier("t1"),
        Join(),
        Identifier("table2"), Identifier("t2"),
        On(),
        Identifier("t1"), Dot(), Identifier("column2"), Equal(), Identifier("t2"), Dot(), Identifier("column3"),
        Where(),
        Identifier("t2"), Dot(), Identifier("column1"), Equal(), Text("value"),
        And(), LeftParenthesis(),
        Identifier("t1"), Dot(), Identifier("column4"), Greater(), Number(3.1f),
        Or(), Identifier("t1"), Dot(), Identifier("column4"), Less(), Number(-3.1f), RightParenthesis(),
        And(), Identifier("t2"), Dot(), Identifier("column5"), Equal(), True(),
        And(), Identifier("t2"), Dot(), Identifier("column6"), Equal(), False()
      )

      val sqlTokens = Lexer(sqlRequest)

      sqlTokens.get

      sqlTokens.isSuccess should be(true)
      sqlTokens.get should be(expectedTokens)
    }

    "fail if tokens are in lower case" in {
      val sqlRequest = "Select * From cars"

      Lexer(sqlRequest).isFailure should be(true)
    }
  }

}
