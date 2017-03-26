package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.plan.physical.InternalRow
import com.octo.mythbuster.spark.Implicits._

import scala.util.Try

package object sql {

  type Name = String

  type ExpressionName = Name

  type RelationName = Name

  type QualifierName = Name

  type ColumnName = Name

  type Row = Map[ColumnName, Any]

  type RowIterable = Iterable[Row]

  type RowIterableRegistry = Map[Name, RowIterable]

  implicit class IterableRegistryImplicits(rowIterableRegistry: RowIterableRegistry) {

    def getRowIterableByName(iterableName: Name): Try[RowIterable] = rowIterableRegistry.get(iterableName).toTry(s"Unable to find the iterable named ${iterableName}")

  }

  implicit class RowImplicits(row: Row) {

    def toInternalRow(relationName: RelationName): InternalRow = row.map({ case (columnName, value) => ((Some(relationName), columnName), value) })

  }

  def table[A](tableName: String)(func: A => Map[ColumnName, Any]): Iterable[A] => Map[RelationName, RowIterable] = { iterable: Iterable[A] =>
    Map(tableName -> iterable.map({ a => func(a) }))
  }

}
