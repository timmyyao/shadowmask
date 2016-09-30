package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.util.DiscoveryUtil;

/**
 * This rule would evaluate whether the column value is an IP address.
 */
public class IPRule extends QusiIdentifierRule{
  public IPRule(RuleContext ruleContext) { super(ruleContext);}

  @Override
  public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException("Should fill the column value before fire inspect rules.");
    }

    String subs[] = value.split("\\.");
    // there are 4 parts after split by '.'
    if (subs.length != 4) {
      return false;
    }
    // all parts consist of only digits and the value of it is between 0 and 255
    for (String sub : subs) {
      if (sub.length() == 0) {
        return false;
      }
      for (int i = 0; i < sub.length(); i ++) {
        if (DiscoveryUtil.isDigitChar(sub.charAt(i)) == false) {
          return false;
        }
      }
      int subVal = Integer.parseInt(sub);
      if (subVal > 255) {
        return false;
      }
    }
    return true;
  }
}
