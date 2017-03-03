package octo.sql.parser

import octo.sql.expression._

/**
  * https://github.com/stephentu/scala-sql-parser/blob/master/src/main/scala/ast.scala
  */
trait AST {

  def toString(toto: String) = toto;

}

trait Relation extends AST

case class Table(tableName: String) extends Relation

case class Select(projections: Seq[Expression], filter: Option[Expression], relations: Seq[Relation]) extends Relation

case class Join(filter: Expression, leftRelation: Relation, rightRelation: Relation) extends Relation
