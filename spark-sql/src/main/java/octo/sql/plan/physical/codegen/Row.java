package octo.sql.plan.physical.codegen;

import com.google.common.collect.Maps;

import java.util.Map;

public class Row {

    private Map<String, Object> valueByColumnName = Maps.newHashMap();

    private Row(Map<String, Object> valueByColumnName) {
        super();
        this.valueByColumnName = valueByColumnName;
    }

    public static Row wrapForJava(Map<String, Object> valueByColumnName) {
        return new Row(valueByColumnName);
    }

    public Map<String, Object> unwrapForScala() {
        return valueByColumnName;
    }

    public Object getValue(String columnName) {
        return valueByColumnName.get(columnName);
    }

}

