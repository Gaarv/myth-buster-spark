package octo.example

import scala.util.parsing.combinator.{PackratParsers, Parsers, RegexParsers}
import scala.{util => u}

// https://gist.github.com/hsanchez/2284734
// http://stackoverflow.com/questions/9785553/how-does-a-simple-calculator-with-parentheses-work
object ArithmeticParser {

   // Expression ::= Term ('+' Term | '-' Term)*
   // Term ::= Factor ('*' Factor | '/' Factor)*
   // Factor ::= ['-'] (Number | '(' Expression ')')
   //Number ::= Digit+

  trait Expression {

  }

  case class Number(value: Int) extends Expression

  case class Add(leftTerm: Expression, rightTerm: Expression) extends Expression

  case class Subtract(leftTerm: Expression, rightTerm: Expression) extends Expression

  case class Multiply(leftFactor: Expression, rightFactor: Expression) extends Expression

  case class Divide(leftFactor: Expression, rightFactor: Expression) extends Expression

  object Parser extends RegexParsers {

    override def skipWhitespace = true

    def expression: Parser[Expression] = term ~ rep("[+-]".r ~ term) ^^ {
      case one ~ others => others.foldLeft(one) {
        case (leftTerm, "+" ~ rightTerm) => Add(leftTerm, rightTerm)
        case (leftTerm, "-" ~ rightTerm) => Subtract(leftTerm, rightTerm)
      }
    }

    def term = factor ~ rep("[*/]".r ~ factor) ^^ {
      case one ~ others => others.foldLeft(one) {
        case (leftFactor, "*" ~ rightFactor) => Multiply(leftFactor, rightFactor)
        case (leftFactor, "/" ~ rightFactor) => Divide(leftFactor, rightFactor)
      }
    }

    def factor = "(" ~> expression <~ ")" | number

    def number = "[0-9]+".r ^^ { t => Number(t.toInt) }

    def apply(t: String): u.Try[Expression] = {
      parse(expression, t) match {
        case NoSuccess(message, _) => u.Failure(new Exception(message))
        case Success(expression, next) => {
          u.Success(expression)
        }
      }
    }

  }


  def main(arguments: Array[String]): Unit = {
    println(Parser("(1 + 2)"))
  }


}
