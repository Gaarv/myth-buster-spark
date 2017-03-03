package octo.sql.lexer

// We import util instead of util.{ Try, Success, Failure } because of the naming collision with the one from Parsers
import scala.util
import scala.util.parsing.combinator.RegexParsers

object Lexer extends RegexParsers {

  override def skipWhitespace: Boolean = true

  def select: Parser[Select] = positioned { "SELECT" ^^ { _ => Select() } }

  def from: Parser[From] = positioned { "FROM" ^^ { _ => From() } }

  def where: Parser[Where] = positioned { "WHERE" ^^ { _ => Where() } }

  def dot: Parser[Dot] = positioned { "." ^^ { _ => Dot() } }

  def equal: Parser[Equal] = positioned { "=" ^^ { _ => Equal() } }

  def identifier: Parser[Identifier] = positioned { "[a-z_][a-z0-9_]*".r ^^  { value => Identifier(value) } }

  def number: Parser[Number] = positioned { "[0-9]+(\\.[0-9]+)?".r ^^ { value => Number(value.toFloat) } }

  def text: Parser[Text] = positioned { "'.*?'".r ^^ { value => Text(value.substring(1, value.length - 1)) } }

  def comma: Parser[Comma] = positioned { "," ^^ { _ => Comma() } }

  def join: Parser[Join] = positioned { "JOIN" ^^ { _ => Join() } }

  def on: Parser[On] = positioned { "ON" ^^ { _ => On() } }

  def greater: Parser[Greater] = positioned { ">" ^^ { _ => Greater() } }

  def less: Parser[Less] = positioned { "<" ^^ { _ => Less() } }

  def or: Parser[Or] = positioned { "OR" ^^ { _ => Or() } }

  def and: Parser[And] = positioned { "AND" ^^ { _ => And() } }

  def `true`: Parser[True] = positioned { "TRUE" ^^ { _ => True() } }

  def `false`: Parser[False] = positioned { "TRUE" ^^ { _ => False() } }

  def leftParenthesis: Parser[LeftParenthesis] = positioned { "(" ^^ { _ => LeftParenthesis() } }

  def rightParenthesis: Parser[RightParenthesis] = positioned { ")" ^^ { _ => RightParenthesis() } }

  def tokens: Parser[Seq[Token]] = { phrase(rep1(select | from | where | dot | equal | `true` | `false` | comma | identifier | join | on | number | text | greater | less | or | and | leftParenthesis | rightParenthesis )) } ^^ { _.toSeq }

  def apply(query: String): util.Try[Seq[Token]] = {
    parse(tokens, query) match {
      case NoSuccess(message, _) => util.Failure(new LexerException(message))
      case Success(tokens, _) => util.Success(tokens)
    }
  }

}
