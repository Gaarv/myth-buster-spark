package octo.sql.expression

import octo.sql.plan.physical.InternalRow
import octo.sql.{ColumnName, TableName}

/**
  * Created by adrien on 3/1/17.
  */
case class TableColumn(tableName: TableName, columnName: ColumnName) extends Expression {

  override type Type = Any

  override def evaluate(row: InternalRow): Type = row((tableName, columnName))

}
