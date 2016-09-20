package com.shadowmask.core.discovery.rules;

import org.junit.Test;
import org.shadowmask.core.discovery.DataDiscoveryException;
import org.shadowmask.core.discovery.RuleContext;
import org.shadowmask.core.discovery.rules.IDRule;
import org.shadowmask.core.type.DataType;

import static org.junit.Assert.assertEquals;

public class IDRuleTest {

    @Test(expected = DataDiscoveryException.class)
    public void testWithoutValue() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.evaluate();
    }

    @Test
    public void testDataType() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.execute();
        assertEquals(DataType.IDENTIFIER, context.getDateType());
    }

    @Test
    public void testWithRightValue() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.setColumnName("id");
        rule.setColumnValue("340001199908180022");
        assertEquals(true, rule.evaluate());
    }

    @Test
    public void testWithWrongValue() {
        RuleContext context = new RuleContext();
        IDRule rule = new IDRule(context);
        rule.setColumnName("id");
        rule.setColumnValue("34000119990818002");
        assertEquals(false, rule.evaluate());
        rule.setColumnValue("3400011999081800222");
        assertEquals(false, rule.evaluate());
        rule.setColumnValue("34000119990818002a");
        assertEquals(false, rule.evaluate());
    }
}
