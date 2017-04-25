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

    public void concatenate(InternalRow concatInternalRow) {
        InternalRow newRow = create();
        newRow.valueByTableNameAndColumName.putAll(valueByTableNameAndColumName);
        newRow.valueByTableNameAndColumName.putAll(concatInternalRow.valueByTableNameAndColumName);
        valueByTableNameAndColumName = newRow.valueByTableNameAndColumName;
//        for(Map.Entry<TableNameAndColumnName, Object> concatEntry : concatInternalRow.valueByTableNameAndColumName.entrySet()) {
//            if(!concatEntry.getKey().getColumnName().equals("rang")) {
//                System.out.println("Entry : " + concatEntry.getKey() + " | " + concatEntry.getValue());
//                valueByTableNameAndColumName.putIfAbsent(concatEntry.getKey(), concatEntry.getValue());
//            }
//        }
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
