package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.sql.{ ColumnName, Name, Row }

package object physical {

  type RelationName = Name

  type InternalColumn = (Option[RelationName], ColumnName)

  type InternalRow = Map[InternalColumn, Any]

  implicit class InternalRowImplicits(internalRow: InternalRow) {

    def toRow: Row = internalRow.map({ case ((tableName, columnName), value) => (tableName.map(n => s"${n}.").getOrElse("") + columnName, value)})

  }

  //val t: Iterator

}
