package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.type.DataType;

public abstract class NonSensitiveDataRule extends MaskBasicRule {

    protected RuleContext context = null;

    public NonSensitiveDataRule(RuleContext context) {
        this.context = context;
        setPriority(4);
    }

    @Override
    public void execute() {
        this.context.setDataType(DataType.NON_SENSITIVE);
    }
}
