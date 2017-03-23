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

  type TableName = Name

  type Row = Map[ColumnName, Any]

  type Table = (Seq[ColumnName], Iterable[Row])

  type TableRegistry = Map[TableName, Table]

  implicit class TableRegistryImplicits(tableRegistry: TableRegistry) {

    def getTableByName(tableName: TableName): Try[Table] = tableRegistry.get(tableName).toTry(s"Unable to find the table named ${tableName}")

  }

  implicit class RowImplicits(row: Row) {

    def toInternalRow(tableName: TableName): InternalRow = row.map({ case (columnName, value) => ((Some(tableName), columnName), value) })

  }

  implicit class TableImplicits(table: Table) {

    def columnNames: Seq[ColumnName] = {
      val (names, _) = table
      names
    }

  }

  def table[A](tableName: String, columnNames: Seq[String])(func: A => Map[ColumnName, Any]): Iterable[A] => Map[TableName, Table] = { iterable: Iterable[A] =>
    Map(tableName -> (columnNames, iterable.map({ a => func(a) })))
  }

}
