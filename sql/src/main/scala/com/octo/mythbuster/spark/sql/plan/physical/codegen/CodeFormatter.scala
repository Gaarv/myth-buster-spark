package com.octo.mythbuster.spark.sql.plan.physical.codegen

/**
  * Created by marc on 29/04/2017.
  */
object CodeFormatter {

  val indentString = "  "

  def format(code : String) : String = {
    var indentCount = 0
    var lastLine = "foo"

    code.split("\n").map(_.trim).flatMap{ line =>
      if(line.isEmpty && lastLine.isEmpty) None
      else {
        val nextLineAdditionalIndent = line.count(isOpenChar) - line.count(isCloseChar)
        val thisLineIndent = indentCount - (if(startsWithCloseChar(line)) 1 else 0)
        indentCount += nextLineAdditionalIndent
        lastLine = line
        Some(indentString * thisLineIndent + line)
      }
    }.zipWithIndex.map({ case (line, index) =>
      s"/* ${pad(index + 1)} */ ${line}"
    }).mkString("\n")
  }

  def isOpenChar(c : Char) = c == '{' || c == '('

  def isCloseChar(c : Char) = c == '}' || c == ')'

  def startsWithCloseChar(s : String) = s.startsWith("}") || s.startsWith(")")

  def pad(i : Int, size : Int = 2) : String = "0" * (size - i.toString.length) + i.toString

}
