package com.shadowmask.core.discovery;

import javafx.util.Pair;
import org.junit.Test;
import org.shadowmask.core.discovery.DataTypeDiscovery;
import org.shadowmask.core.type.DataType;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class DataTypeDiscoveryTest {

    @Test
    public void testDiscovery1() {
        // prepare data
        List<Pair<String, String>> input = new LinkedList<>();
        input.add(new Pair<>("id", "340001199908180022"));
        input.add(new Pair<>("age", "18"));
        input.add(new Pair<>("sport", "football"));

        // inspect with rule engine
        List<DataType> dataTypes = DataTypeDiscovery.inspectTypes(input);

        // verify result
        assertEquals(3, dataTypes.size());
        assertEquals(DataType.IDENTIFIER, dataTypes.get(0));
        assertEquals(DataType.QUSI_IDENTIFIER, dataTypes.get(1));
        assertEquals(DataType.NON_SENSITIVE, dataTypes.get(2));
    }
}
