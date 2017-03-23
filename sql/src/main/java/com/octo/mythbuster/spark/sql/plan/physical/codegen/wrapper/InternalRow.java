package com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper;

import java.util.HashMap;
import java.util.Map;

public class InternalRow {

    private Map<TableNameAndColumnName, Object> valueByTableNameAndColumName;

    private InternalRow(Map<TableNameAndColumnName, Object> valueByTableNameAndColumName) {
        super();
        this.valueByTableNameAndColumName = valueByTableNameAndColumName;
    }

    public static InternalRow wrap(Map<TableNameAndColumnName, Object> valueByTableNameAndColumName) {
        return new InternalRow(valueByTableNameAndColumName);
    }

    public Map<TableNameAndColumnName, Object> unwrap() {
        return valueByTableNameAndColumName;
    }

    public Object getValue(TableNameAndColumnName tableNameAndColumnName) {
        Object value = valueByTableNameAndColumName.get(tableNameAndColumnName);
        if (value == null) throw new RuntimeException("There is no " + tableNameAndColumnName + " column");
        return value;
    }

    public void setValue(TableNameAndColumnName tableNameAndColumnName, Object value) {
        this.valueByTableNameAndColumName.put(tableNameAndColumnName, value);
    }

    public static InternalRow create() {
        return new InternalRow(new HashMap<>());
    }

    @Override
    public String toString() {
        return "InternalRow(" + valueByTableNameAndColumName + ")";
    }

}
