package octo.sql.codegen;

import java.util.Objects;

public class TableNameAndColumnName {

    private String tableName;
    private String columnName;

    private TableNameAndColumnName(String tableName, String columnName) {
        super();

        this.tableName = tableName;
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public static TableNameAndColumnName of(String tableName, String columnName) {
        return new TableNameAndColumnName(tableName, columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableName, columnName);
    }

    public boolean equals(Object object) {
        boolean equality = false;
        if (object instanceof TableNameAndColumnName) {
            TableNameAndColumnName that = (TableNameAndColumnName) object;
            equality = Objects.equals(this.tableName, that.tableName) && Objects.equals(this.columnName, that.columnName);
        }
        return equality;
    }

}
