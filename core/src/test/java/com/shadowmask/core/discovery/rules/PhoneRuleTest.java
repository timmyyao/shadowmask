package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.PhoneRule;
import org.shadowmask.core.type.DataType;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 9/30/16.
 */
public class PhoneRuleTest {

  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    PhoneRule rule = new PhoneRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    PhoneRule rule = new PhoneRule(context);
    rule.execute();
    assertEquals(DataType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    PhoneRule rule = new PhoneRule(context);
    rule.setColumnName("phone");
    rule.setColumnValue("21-66666666");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("231-66666666");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    PhoneRule rule = new PhoneRule(context);
    rule.setColumnName("phone");
    rule.setColumnValue("21-66c66666");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1-66666666");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1345-66666666");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("66666666");
    assertEquals(false, rule.evaluate());
  }
}
