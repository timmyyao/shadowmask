package org.shadowmask.core.discovery.rules;

import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;

/**
 * This rule would evaluate whether the column value is an email address.
 */
public class EmailRule extends QusiIdentifierRule{
  public EmailRule(RuleContext ruleContext) { super(ruleContext);}

  @Override
  public boolean evaluate() {
    if (value == null) {
      throw new DataDiscoveryException("Should fill the column value before fire inspect rules.");
    }

    int locationOfAt = value.indexOf('@');
    // there is only one '@' in the value and not in the beginning
    if (locationOfAt > 0 && locationOfAt == value.lastIndexOf('@')) {
      int locationOfDot = value.lastIndexOf('.');
      // there is a '.' after '@'
      if (locationOfDot > locationOfAt) {
        return true;
      }
    }
    return false;
  }
}
