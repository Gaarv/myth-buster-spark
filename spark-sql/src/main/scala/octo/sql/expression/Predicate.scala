package octo.sql.expression
import scala.util.{Success, Try}

trait Predicate extends Expression {

  override type Type = Boolean

  override def toPredicate: Try[Predicate] = Success(this)

}
