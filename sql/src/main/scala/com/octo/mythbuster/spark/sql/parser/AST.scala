package com.octo.mythbuster.spark.sql.parser

import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.expression.{BinaryOperation, Expression}

trait AST

trait Relation extends AST

case class Table(name: RelationName) extends Relation

case class Select(projections: Seq[Expression], filter: Option[Expression], relations: Seq[Relation]) extends Relation

case class Join(filter: BinaryOperation, leftRelation: Relation, rightRelation: Relation) extends Relation
