package octo.sql.plan

import scala.util.Try

package object physical {

  type InternalRow = Map[(Option[TableName], ColumnName), Any]

  implicit class InternalRowImplicits(internalRow: InternalRow) {

    def toRow: Row = internalRow.map({ case ((tableName, columnName), value) => (tableName.map(n => s"${n}.").getOrElse("") + columnName, value)})

  }

}
