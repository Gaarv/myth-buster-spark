package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.sql.{ ColumnName, Row, TableName }

package object physical {

  type InternalField = (Option[TableName], ColumnName)

  type InternalRow = Map[InternalField, Any]

  implicit class InternalRowImplicits(internalRow: InternalRow) {

    def toRow: Row = internalRow.map({ case ((tableName, columnName), value) => (tableName.map(n => s"${n}.").getOrElse("") + columnName, value)})

  }

}
