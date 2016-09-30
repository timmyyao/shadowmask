package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.util.DiscoveryUtil;

/**
 * This rule would evaluate whether the column value is chinese citizen id.
 */
public class IDRule extends IdentifierRule {

    public IDRule(RuleContext context) {
        super(context);
    }

    @Override
    public boolean evaluate() {
        if (value == null) {
            throw new DataDiscoveryException("Should fill the column value before fire inspect rules.");
        }

        if (value.length() == 18) {
            boolean result = true;
            int i;
            for (i=0; i<value.length()-1; i++) {
                result = result && DiscoveryUtil.isDigitChar(value.charAt(i));
            }
            // last digit of ID code may be 'x' or 'X'
            result = result && (DiscoveryUtil.isDigitChar(value.charAt(i)) ||
              value.charAt(i) == 'x' || value.charAt(i) == 'X');
            return result;
        }

        return false;
    }


}
