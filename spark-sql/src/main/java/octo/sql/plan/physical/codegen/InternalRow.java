package octo.sql.plan.physical.codegen;

import com.google.common.collect.Maps;

import java.util.Map;

public class InternalRow {

    private Map<TableNameAndColumnName, Object> valueByTableNameAndColumName = Maps.newHashMap();

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
        return valueByTableNameAndColumName.get(valueByTableNameAndColumName);
    }

}
