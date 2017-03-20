package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.sql.{ ColumnName, Row, TableName }

import scala.util.Try

package object physical {

  type InternalRow = Map[(Option[TableName], ColumnName), Any]

  implicit class PhysicalRowImplicits(physicalRow: InternalRow) {

    def toRow: Row = physicalRow.map({ case ((tableName, columnName), value) => (tableName.map(n => s"${n}.").getOrElse("") + columnName, value)})

  }

}
