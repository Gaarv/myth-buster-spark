package com.octo.mythbuster.spark

import com.octo.mythbuster.spark.sql.plan.physical.InternalRow

package object sql {

  type Name = String

  type ExpressionName = Name

  type RelationName = Name

  type QualifierName = Name

  type ColumnName = Name

  type Row = Map[ColumnName, Any]


  implicit class RowImplicits(row: Row) {

    def toInternalRow(relationName: RelationName): InternalRow = row.map({ case (columnName, value) => ((Some(relationName), columnName), value) })

  }

}
