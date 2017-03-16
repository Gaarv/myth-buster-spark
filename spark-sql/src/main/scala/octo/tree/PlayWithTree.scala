package octo.tree

import java.util

import scala.reflect._



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

// 5 * (1 + 2) = 5 * 1 + 5 * 2
object Factorizable {

  def unapply(ast: AST): Option[(AST, (AST, AST))] = ast match {

    case _ => None
  }

}

object PlayWithTree {

  def main(arguments: Array[String]): Unit = {

    // 2 * -76 + 2 * 34 --> 2 * (-76 + 34)
    var ast: AST = Add(Multiply(Number(2), Opposite(Number(76))), Multiply(Number(2), Number(34)))
    ast = ast.transformDown({
      case Add(Multiply(a, b), Multiply(c, d)) if a == c => Multiply(a, Add(b, c))
    })

    println(ast)
    println(ast.evaluate())
  }

}