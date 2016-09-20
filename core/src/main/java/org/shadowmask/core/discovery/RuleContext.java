package org.shadowmask.core.discovery;

import org.shadowmask.core.type.DataType;

public class RuleContext {

    private DataType dateType = DataType.NON_SENSITIVE;

    public void initiate() {
        this.dateType = DataType.NON_SENSITIVE;
    }

    public void setDataType(DataType dataType) {
        this.dateType = dataType;
    }

    public DataType getDateType() {
        return this.dateType;
    }
}
