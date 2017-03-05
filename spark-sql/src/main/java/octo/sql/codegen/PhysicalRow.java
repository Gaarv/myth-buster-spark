package octo.sql.codegen;

import com.google.common.collect.Maps;

import java.util.Map;

public class PhysicalRow {

    private Map<TableNameAndColumnName, Object> valueByTableNameAndColumName = Maps.newHashMap();

    private PhysicalRow(Map<TableNameAndColumnName, Object> valueByTableNameAndColumName) {
        super();
        this.valueByTableNameAndColumName = valueByTableNameAndColumName;
    }

    public static PhysicalRow wrapForJava(Map<TableNameAndColumnName, Object> valueByTableNameAndColumName) {
        return new PhysicalRow(valueByTableNameAndColumName);
    }

    public Map<TableNameAndColumnName, Object> unwrapForScala() {
        return valueByTableNameAndColumName;
    }

    public Object getValue(TableNameAndColumnName tableNameAndColumnName) {
        return valueByTableNameAndColumName.get(valueByTableNameAndColumName);
    }

}
