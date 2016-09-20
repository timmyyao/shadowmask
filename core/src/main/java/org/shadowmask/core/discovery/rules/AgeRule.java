package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;

public class AgeRule extends QusiIdentifierRule {

    public AgeRule(RuleContext ruleContext) {
        super(ruleContext);
    }

    @Override
    public boolean evaluate() {
        if (value == null) {
            throw new DataDiscoveryException("Should fill the column value before fire inspect rules.");
        }

        if (name == null) {
            return false;
        } else if (name.equalsIgnoreCase("age")) {
            return true;
        } else {
            return false;
        }
    }
}
