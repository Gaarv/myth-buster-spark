package com.octo.mythbuster.spark.tree

import com.octo.mythbuster.spark.UnitSpec
import sample._

class TreeSpec extends UnitSpec {

  val originalAST = Add(
    Multiply(
      Number(2),
      Number(4)
    ),
    Multiply(
      Add(
        Number(3),
        Number(5)
      ),
      Number(4)
    )
  )

  val factorizedAST = Multiply(
    Number(4),
    Add(
      Number(2),
      Add(
        Number(3),
        Number(5)
      )
    )
  )

  // a * b + c * a = a * (b + c)
  object Factorize {

    def unapply(ast: AST): Option[AST] = ast match {
      case Add(Multiply(a, b), Multiply(c, d)) => (a, b, c, d) match {
        case _ if a == c => Some(Multiply(a, Add(b, d)))
        case _ if a == d => Some(Multiply(a, Add(b, c)))
        case _ if b == c => Some(Multiply(b, Add(a, d)))
        case _ if b == d => Some(Multiply(b, Add(a, c)))
        case _ => None
      }
      case _ => None
    }

  }

  "The original AST" should "be factorizable" in {
    val transformedAST = originalAST.transformDown({
      case Factorize(factorizedAST) => factorizedAST
    })

    transformedAST should be(factorizedAST)
  }

  it should "be evaluated properly" in {
    val evaluatedResult = originalAST.evaluate()
    evaluatedResult should be(40)
  }

}
