package octo.tree

import java.util

import scala.reflect._



trait AST extends TreeNode[AST] {

  def evaluate(): Float

}

trait BinaryAST extends AST with BinaryTreeNode[AST] {



}

trait LeafAST extends AST with LeafTreeNode[AST] {

}

trait Operation extends BinaryAST {

  type Operator = (Float, Float) => Float

  val operator: Operator

  override def evaluate() = operator(leftChild.evaluate(), rightChild.evaluate())

}

case class Add(leftChild: AST, rightChild: AST) extends Operation {

  val operator: Operator = { _ + _ }

}

case class Multiply(leftChild: AST, rightChild: AST) extends Operation {

  val operator: Operator = { _ * _ }

}

case class Number(val value: Float) extends LeafAST {

  override def evaluate() = value

}

// 5 * (1 + 2) = 5 * 1 + 5 * 2
object Factorizable {

  def unapply(ast: AST): Option[(AST, (AST, AST))] = ast match {
    case Add(Multiply(a, b), Multiply(c, d)) if a == c => Some((a, (b, d)))
    case _ => None
  }

}

object PlayWithTree {

  def main(arguments: Array[String]): Unit = {
    var ast: AST = Add(Multiply(Number(2), Number(76)), Multiply(Number(2), Number(34)))
    ast = ast.transformDown({
      case ast @ Factorizable((a, (b, c))) => Multiply(a, Add(b, c))
    })

    println(ast)
    println(ast.evaluate())
  }

}