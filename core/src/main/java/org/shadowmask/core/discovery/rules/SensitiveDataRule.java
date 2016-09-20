package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.type.DataType;

public abstract class SensitiveDataRule extends MaskBasicRule {

    protected RuleContext context = null;

    public SensitiveDataRule(RuleContext context) {
        this.context = context;
        setPriority(3);
    }

    @Override
    public void execute() {
        this.context.setDataType(DataType.SENSITIVE);
    }
}
