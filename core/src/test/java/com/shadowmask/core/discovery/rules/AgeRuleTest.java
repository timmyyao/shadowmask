package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.AgeRule;
import org.shadowmask.core.type.DataType;

import static org.junit.Assert.assertEquals;

public class AgeRuleTest {

    @Test (expected = DataDiscoveryException.class)
    public void testWithoutValue() {
        RuleContext context = new RuleContext();
        AgeRule rule = new AgeRule(context);
        rule.evaluate();
        rule.execute();
    }

    @Test
    public void testDataType() {
        RuleContext context = new RuleContext();
        AgeRule rule = new AgeRule(context);
        rule.execute();
        assertEquals(DataType.QUSI_IDENTIFIER, context.getDateType());
    }

    @Test
    public void testWithRightColumnName() {
        RuleContext context = new RuleContext();
        AgeRule rule = new AgeRule(context);
        rule.setColumnName("age");
        rule.setColumnValue("18");
        assertEquals(true, rule.evaluate());
    }

    @Test
    public void testWithOtherColumnName() {
        RuleContext context = new RuleContext();
        AgeRule rule = new AgeRule(context);
        rule.setColumnName("stu_age");
        rule.setColumnValue("18");
        assertEquals(false, rule.evaluate());
    }
}
