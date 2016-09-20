package org.shadowmask.core.discovery.rules;

import org.easyrules.core.BasicRule;

public class MaskBasicRule extends BasicRule {

    protected String name;
    protected String value;

    public void setColumnName(String columnName) {
        this.name = columnName;
    }

    public void setColumnValue(String columnValue) {
        this.value = columnValue;
    }
}
