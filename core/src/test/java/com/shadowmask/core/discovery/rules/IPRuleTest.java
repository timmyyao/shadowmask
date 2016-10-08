package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.IPRule;
import org.shadowmask.core.type.DataType;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 9/30/16.
 */
public class IPRuleTest {

  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    IPRule rule = new IPRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    IPRule rule = new IPRule(context);
    rule.execute();
    assertEquals(DataType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    IPRule rule = new IPRule(context);
    rule.setColumnName("ip");
    rule.setColumnValue("10.92.0.255");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    IPRule rule = new IPRule(context);
    rule.setColumnName("ip");
    rule.setColumnValue("10.9.3");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("10.9.3.6.7");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("10.9..9");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue(".10.9.3");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("10.9.3b.9");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("245.4.280.0");
    assertEquals(false, rule.evaluate());
  }
}
