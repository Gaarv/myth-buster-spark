package com.octo.mythbuster.spark.tree.sample

import com.octo.mythbuster.spark.tree._

trait AST extends TreeNode[AST] {

  def evaluate(): Float

}

trait BinaryAST extends AST with BinaryTreeNode[AST]

trait LeafAST extends AST with LeafTreeNode[AST]

trait UnaryAST extends AST with UnaryTreeNode[AST]

trait UnaryArityOperation extends UnaryAST {

  type Operator = (Float) => Float

  val operator: Operator

  override def evaluate() = operator(child.evaluate())

}

trait BinaryArityOperation extends BinaryAST {

  type Operator = (Float, Float) => Float

  val operator: Operator

  override def evaluate() = operator(leftChild.evaluate(), rightChild.evaluate())

}

case class Add(leftChild: AST, rightChild: AST) extends BinaryArityOperation {

  val operator: Operator = { _ + _ }

}

case class Opposite(child: AST) extends UnaryArityOperation {

  val operator = { -1 * _ }

}

case class Multiply(leftChild: AST, rightChild: AST) extends BinaryArityOperation {

  val operator: Operator = { _ * _ }

}

case class Number(val value: Float) extends LeafAST {

  override def evaluate() = value

}

