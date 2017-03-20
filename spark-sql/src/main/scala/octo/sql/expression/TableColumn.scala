package octo.sql.expression

/**
  * Created by adrien on 3/1/17.
  */
case class TableColumn(tableName: TableName, columnName: ColumnName) extends Expression {

  override type Type = Any

  override def evaluate(row: InternalRow): Type = row((Some(tableName), columnName))

  override def generateCode(javaVariableName: String): String = {
    s"""${javaVariableName}.getValue(TableNameAndColumnName.of(Optional.of("${tableName}"), "${columnName}"))"""
  }

}
