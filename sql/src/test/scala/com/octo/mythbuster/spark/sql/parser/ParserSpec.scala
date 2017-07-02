package com.octo.mythbuster.spark.sql.parser

import com.octo.mythbuster.spark.UnitSpec
import com.octo.mythbuster.spark.sql.expression._
import com.octo.mythbuster.spark.sql.lexer.Identifier
import com.octo.mythbuster.spark.sql.{parser => p}
import com.octo.mythbuster.spark.sql.{lexer => l}
import org.scalatest.{WordSpec, Matchers}

/**
  * Created by marc on 02/07/2017.
  */
class ParserSpec extends WordSpec with Matchers  {

  "Parser applied to a Select * request" should {
    "return an AST with no expression if there is no filter" in {
      // SELECT * FROM table
      val inputTokens = List(l.Select(), l.Star(), l.From(), l.Identifier("table"))

      val outputAst = Parser(inputTokens)
      val expectedAst = SelectStar(None, Seq(Table("table")))

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }

    "return an AST with an expression if there is a filter" in {
      // SELECT * FROM table WHERE table.column='value'
      val inputTokens = List(
        l.Select(), l.Star(),
        l.From(), l.Identifier("table"),
        l.Where(), l.Identifier("table"), l.Dot(), l.Identifier("column"), l.Equal(), l.Text("value"))

      val outputAst = Parser(inputTokens)
      val expectedAst = SelectStar(
        Some(
          Equal(
            TableColumn("table", "column"),
            Text("value")
          )),
        Seq(Table("table")))

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }
  }

  "Parser applied to a Select table.column request" should {
    "return an AST with no expression if there is no filter and several TableColumns if there are several projections" in {
      // SELECT table.column1, table.column2 FROM table
      val inputTokens = List(
        l.Select(),
          l.Identifier("table"), l.Dot(), l.Identifier("column1"), l.Comma(),
          l.Identifier("table"), l.Dot(), l.Identifier("column2"),
        l.From(), l.Identifier("table"))

      val outputAst = Parser(inputTokens)
      val expectedAst = Select(
        Seq(
          TableColumn("table", "column1"),
          TableColumn("table", "column2")
        ),
        None,
        Seq(Table("table"))
      )

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }

    "return an AST with an expression if there is a filter" in {
      // SELECT table.column FROM table WHERE table.column='value'
      val inputTokens = List(
        l.Select(), l.Identifier("table"), l.Dot(), l.Identifier("column"),
        l.From(), l.Identifier("table"),
        l.Where(), l.Identifier("table"), l.Dot(), l.Identifier("column"), l.Equal(), l.Text("value"))

      val outputAst = Parser(inputTokens)
      val expectedAst = Select(
        Seq(TableColumn("table", "column")),
        Some(
          Equal(
            TableColumn("table", "column"),
            Text("value")
          )),
        Seq(Table("table")))

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }

    "return an AST with several expressions if there are several filters with parenthesis" in {
      // SELECT table.column FROM table WHERE table.column1='value' AND (table.column2 > 3.0 OR table.column2 < -3.0)
      val inputTokens = List(
        l.Select(), l.Identifier("table"), l.Dot(), l.Identifier("column"),
        l.From(), l.Identifier("table"),
        l.Where(), l.Identifier("table"), l.Dot(), l.Identifier("column1"), l.Equal(), l.Text("value"),
        l.And(), l.LeftParenthesis(),
          l.Identifier("table"), l.Dot(), l.Identifier("column2"), l.Greater(), l.Number(3.0f),
          l.Or(), l.Identifier("table"), l.Dot(), l.Identifier("column2"), l.Less(), l.Number(-3.0f),
        l.RightParenthesis()
      )

      val outputAst = Parser(inputTokens)
      val expectedAst = Select(
        Seq(TableColumn("table", "column")),
        Some(
          And(
            Equal(
              TableColumn("table", "column1"),
              Text("value")),
            Or(
              Greater(
                TableColumn("table", "column2"),
                Number(3.0f)),
              Less(
                TableColumn("table", "column2"),
                Number(-3.0f))
            ))
        ),
        Seq(Table("table")))

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }

    "return an AST with an alias if there is AS token" in {
      // SELECT table.column FROM table AS t
      val inputTokens = List(
        l.Select(), l.Identifier("table"), l.Dot(), l.Identifier("column"),
        l.From(), l.Identifier("table"), l.As(), l.Identifier("t")
      )

      val outputAst = Parser(inputTokens)
      val expectedAst = Select(
        Seq(TableColumn("table", "column")),
        None,
        Seq(p.Alias(Table("table"), "t"))
      )

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }

    "return an AST with an alias without AS token" in {
      // SELECT table.column FROM table t
      val inputTokens = List(
        l.Select(), l.Identifier("table"), l.Dot(), l.Identifier("column"),
        l.From(), l.Identifier("table"), l.Identifier("t")
      )

      val outputAst = Parser(inputTokens)
      val expectedAst = Select(
        Seq(TableColumn("table", "column")),
        None,
        Seq(p.Alias(Table("table"), "t"))
      )

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }

    "return an AST with a Join if there is a Join Token" in {
      // SELECT table1.column1 FROM table1 JOIN table2 ON table1.column1 = table2.column1
      val inputTokens = List(
        l.Select(), l.Identifier("table1"), l.Dot(), l.Identifier("column1"),
        l.From(), l.Identifier("table1"),
        l.Join(), l.Identifier("table2"),
        l.On(), l.Identifier("table1"), l.Dot(), l.Identifier("column1"),
          l.Equal(), l.Identifier("table2"), l.Dot(), l.Identifier("column1")
      )

      val outputAst = Parser(inputTokens)
      val expectedAst = Select(
        Seq(TableColumn("table1", "column1")),
        None,
        Seq(Join(
          Equal(
            TableColumn("table1", "column1"),
            TableColumn("table2", "column1")
          ),
          Table("table1"),
          Table("table2"))
        )
      )

      outputAst.isSuccess should be(true)
      outputAst.get should be(expectedAst)
    }
  }

}
