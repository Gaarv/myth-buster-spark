package octo

import octo.Implicits._
import octo.sql.plan.physical.InternalRow

import scala.util.Try

package object sql {

  type Name = String

  type ColumnName = Name

  type TableName = Name

  type Row = Map[ColumnName, Any]

  type Table = Iterable[Row]

  type TableRegistry = Map[TableName, Table]

  implicit class TableRegistryImplicits(tableRegistry: TableRegistry) {

    def getTableByName(tableName: TableName): Try[Table] = tableRegistry.get(tableName).toTry(s"Unable to find the table named ${tableName}")

  }

  implicit class RowImplicits(row: Row) {

    def toPhysicalRow(tableName: TableName): InternalRow = row.map({ case (columnName, value) => ((tableName, columnName), value) })

  }

  def table[A](tableName: TableName)(func: A => Map[ColumnName, Any]): Iterable[A] => Map[TableName, Table] = { iterable: Iterable[A] =>
    Map(tableName -> iterable.map({ a => func(a) }))
  }

}
