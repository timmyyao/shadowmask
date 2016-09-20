package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.type.DataType;

public abstract class QusiIdentifierRule extends MaskBasicRule {

    protected RuleContext context = null;

    public QusiIdentifierRule(RuleContext context) {
        this.context = context;
        setPriority(2);
    }

    @Override
    public void execute() {
        this.context.setDataType(DataType.QUSI_IDENTIFIER);
    }
}
