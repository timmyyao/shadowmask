package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.MobileRule;
import org.shadowmask.core.type.DataType;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 9/30/16.
 */
public class MobileRuleTest {

  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.execute();
    assertEquals(DataType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.setColumnName("mobile");
    rule.setColumnValue("12312345678");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    MobileRule rule = new MobileRule(context);
    rule.setColumnName("mobile");
    rule.setColumnValue("123123456789");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1231234567");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("1231234567x");
    assertEquals(false, rule.evaluate());
  }

}
