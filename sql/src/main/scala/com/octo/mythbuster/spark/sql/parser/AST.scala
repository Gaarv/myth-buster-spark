package com.octo.mythbuster.spark.sql.parser

import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.expression.Expression

trait AST

trait Relation extends AST

trait NamedRelation extends Relation {

  val name: RelationName

}

case class Table(name: RelationName) extends NamedRelation

case class Alias(relation: Relation, name: RelationName) extends NamedRelation

case class Select(projections: Seq[Expression], filter: Option[Expression], relations: Seq[Relation]) extends Relation

case class Join(filter: Expression, leftRelation: Relation, rightRelation: Relation) extends Relation
