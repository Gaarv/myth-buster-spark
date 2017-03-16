package octo.sql.plan.physical.codegen.spi;

import java.util.Objects;
import java.util.Optional;

public class TableNameAndColumnName {

    private Optional<String> tableName;
    private String columnName;

    private TableNameAndColumnName(Optional<String> tableName, String columnName) {
        super();

        this.tableName = tableName;
        this.columnName = columnName;
    }

    public Optional<String> getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public static TableNameAndColumnName of(Optional<String> tableName, String columnName) {
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

    @Override
    public String toString() {
        return "TableNameAndColumnName(" + tableName + ", " + columnName + ")";
    }

}
