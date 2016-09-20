package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.type.DataType;

public abstract class IdentifierRule extends MaskBasicRule {

    protected RuleContext context = null;

    public IdentifierRule(RuleContext context) {
        this.context = context;
        setPriority(1);
    }

    @Override
    public void execute() {
        this.context.setDataType(DataType.IDENTIFIER);
    }
}
