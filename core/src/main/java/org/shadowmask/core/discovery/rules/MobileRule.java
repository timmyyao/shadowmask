package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.util.DiscoveryUtil;

/**
 * This rule would evaluate whether the column value is a mobile number.
 */
public class MobileRule extends QusiIdentifierRule {
  public MobileRule(RuleContext ruleContext) { super(ruleContext);}

  @Override
  public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException("Should fill the column value before fire inspect rules.");
    }

    // 11-digit long, contain only digits
    if (value.length() == 11) {
      boolean result = true;
      int i;
      for (i=0; i<value.length(); i++) {
        result = result && DiscoveryUtil.isDigitChar(value.charAt(i));
      }
      return result;
    }

    return false;
  }
}
