package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.util.DiscoveryUtil;

/**
 * This rule would evaluate whether the column value is a chinese phone number.
 */
public class PhoneRule extends QusiIdentifierRule{
  public PhoneRule(RuleContext ruleContext) { super(ruleContext);}

  @Override
  public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException("Should fill the column value before fire inspect rules.");
    }

    String subs[] = value.split("\\-");
    // there are 2 parts after splitting by '-', e.g. 021-88888888
    if (subs.length != 2) {
      return false;
    }
    // all parts consist of only digits and the first part should be 2 or 3 digits long
    if (subs[0].length() > 3 || subs[0].length() < 2) {
      return false;
    }
    for (String sub : subs) {
      for (int i = 0; i < sub.length(); i ++) {
        if (DiscoveryUtil.isDigitChar(sub.charAt(i)) == false) {
          return false;
        }
      }
    }
    return true;
  }
}
