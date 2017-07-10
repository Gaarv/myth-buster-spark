package com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper;

import java.util.HashMap;
import java.util.Map;

public class InternalRow {

    private Map<String, Object> valueByTableNameAndColumName;

    private InternalRow(Map<String, Object> valueByTableNameAndColumName) {
        super();
        this.valueByTableNameAndColumName = valueByTableNameAndColumName;
    }

    public static InternalRow wrap(Map<String, Object> valueByTableNameAndColumName) {
        return new InternalRow(valueByTableNameAndColumName);
    }

    public Map<String, Object> unwrap() {
        return valueByTableNameAndColumName;
    }

    public InternalRow concatenate(InternalRow that) {
        InternalRow concatenatedInternalRow = create();
        concatenatedInternalRow.valueByTableNameAndColumName.putAll(this.valueByTableNameAndColumName);
        concatenatedInternalRow.valueByTableNameAndColumName.putAll(that.valueByTableNameAndColumName);
        return concatenatedInternalRow;
//        for(Map.Entry<String, Object> concatEntry : concatInternalRow.valueByTableNameAndColumName.entrySet()) {
//            if(!concatEntry.getKey().getColumnName().equals("rang")) {
//                System.out.println("Entry : " + concatEntry.getKey() + " | " + concatEntry.getValue());
//                valueByTableNameAndColumName.putIfAbsent(concatEntry.getKey(), concatEntry.getValue());
//            }
//        }
    }

    public Object getValue(String tableNameAndColumnName) {
        Object value = valueByTableNameAndColumName.get(tableNameAndColumnName);
        if (value == null) throw new RuntimeException("There is no " + tableNameAndColumnName + " column in " + this.valueByTableNameAndColumName);
        return value;
    }

    public void setValue(String tableNameAndColumnName, Object value) {
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
