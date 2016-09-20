package org.shadowmask.core.discovery;

import javafx.util.Pair;
import org.easyrules.api.Rule;
import org.easyrules.api.RulesEngine;
import org.easyrules.core.RulesEngineBuilder;
import org.shadowmask.core.discovery.rules.AgeRule;
import org.shadowmask.core.discovery.rules.IDRule;
import org.shadowmask.core.discovery.rules.MaskBasicRule;
import org.shadowmask.core.type.DataType;

import java.util.LinkedList;
import java.util.List;

public class DataTypeDiscovery {

    private static RuleContext context = new RuleContext();
    private static RulesEngine engine;

    static {
        RulesEngineBuilder builder = RulesEngineBuilder.aNewRulesEngine();
        // once a rule is applied, it means that we are confident that the data type is found, so
        // just skip the following rules.
        engine = builder.named("DataTypeAutoDiscovery")
                .withSkipOnFirstAppliedRule(true)
                .build();
        engine.registerRule(new IDRule(context));
        engine.registerRule(new AgeRule(context));
    }

    public static synchronized List<DataType> inspectTypes(List<Pair<String, String>> input) {
        List<DataType> dataTypes = new LinkedList<>();
        for(Pair<String, String> pair : input) {
            // set the column name/value for rule to evaluate.
            for (Rule rule: engine.getRules()) {
                MaskBasicRule maskRule = (MaskBasicRule)rule;
                maskRule.setColumnName(pair.getKey());
                maskRule.setColumnValue(pair.getValue());
            }
            engine.fireRules();
            dataTypes.add(context.getDateType());
            context.initiate();
         }
        return dataTypes;
    }

}
