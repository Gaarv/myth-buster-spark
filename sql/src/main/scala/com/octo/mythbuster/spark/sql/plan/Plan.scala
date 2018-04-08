package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.sql.expression.Expression
import com.octo.mythbuster.spark.{Logging, tree => t}

// A plan can be either logical or physical, and inherits all the properties of a TreeNode (in order to simplify the optimization)
trait Plan[P <: Plan[P]] extends t.TreeNode[P] with Logging {
  self: P =>

  def explain(highlight: String = ""): String = {
    val explained = explain(0)
    if (highlight.isEmpty) explained
    else explained.replace(highlight, s"${Console.GREEN_B}${highlight}${Console.RESET}")
  }

  private [sql] def explain(indent: Int): String = {
    this match {
      case binaryTreeNode: t.BinaryTreeNode[P] =>
        s"""${"  " * indent}${titleForExplain}
           |${binaryTreeNode.leftChild.explain(indent + 1)}
           |${binaryTreeNode.rightChild.explain(indent + 1)}""".stripMargin

      case unaryTreeNode: t.UnaryTreeNode[P] =>
        s"""${"  " * indent}${titleForExplain}
           |${unaryTreeNode.child.explain(indent + 1)}""".stripMargin

      case leafTreeNode: t.LeafTreeNode[P] =>
        s"""${"  " * indent}${titleForExplain}"""

      case _ =>
        throw new UnsupportedOperationException(s"Unable to handle ${titleForExplain}")
    }
  }

  private[sql] def titleForExplain: String = {
    s"${getClass.getSimpleName}()"
  }

  def toSting(): String = titleForExplain

}