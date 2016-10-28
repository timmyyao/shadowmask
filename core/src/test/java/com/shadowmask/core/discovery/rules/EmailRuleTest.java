package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.EmailRule;
import org.shadowmask.core.type.DataType;

import static org.junit.Assert.assertEquals;

/**
 * Created by root on 9/30/16.
 */
public class EmailRuleTest {

  @Test(expected = DataDiscoveryException.class)
  public void testWithoutValue() {
    RuleContext context = new RuleContext();
    EmailRule rule = new EmailRule(context);
    rule.evaluate();
  }

  @Test
  public void testDataType() {
    RuleContext context = new RuleContext();
    EmailRule rule = new EmailRule(context);
    rule.execute();
    assertEquals(DataType.QUSI_IDENTIFIER, context.getDateType());
  }

  @Test
  public void testWithRightValue() {
    RuleContext context = new RuleContext();
    EmailRule rule = new EmailRule(context);
    rule.setColumnName("email");
    rule.setColumnValue("zhangsan123@xxx.com");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("Zhang_san@xxx.com.cn");
    assertEquals(true, rule.evaluate());
    rule.setColumnValue("zhang.san@xxx.org");
    assertEquals(true, rule.evaluate());
  }

  @Test
  public void testWithWrongValue() {
    RuleContext context = new RuleContext();
    EmailRule rule = new EmailRule(context);
    rule.setColumnName("email");
    rule.setColumnValue("zhangsan");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("zhangsan@xx@yy.com");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("zhangsan@xx.sss");
    assertEquals(false, rule.evaluate());
    rule.setColumnValue("zhang.san@xx");
    assertEquals(false, rule.evaluate());
  }
}
