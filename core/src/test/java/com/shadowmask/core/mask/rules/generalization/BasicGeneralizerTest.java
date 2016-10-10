/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shadowmask.core.mask.rules.generalization;

import org.junit.Test;
import org.shadowmask.core.mask.rules.MaskRuntimeException;
import org.shadowmask.core.mask.rules.generalizer.impl.*;

import static org.junit.Assert.assertEquals;

public class BasicGeneralizerTest {

    @Test(expected = MaskRuntimeException.class)
    public void stringGeneralizationInvalidHierarchyLevel1() {
        StringGeneralizer generalization = new StringGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        String input = "123456789";
        generalization.generalize(input, -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void stringGeneralizationInvalidHierarchyLevel2() {
        StringGeneralizer generalization = new StringGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        String input = "123456789";
        generalization.generalize(input, 6);
    }

    @Test
    public void stringGeneralizationTest1() {
        StringGeneralizer generalization = new StringGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        String input = "123456789";
        String level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        String level1 = generalization.generalize(input, 1);
        assertEquals("12345678", level1);
        String level2 = generalization.generalize(input, 2);
        assertEquals("1234567", level2);
        String level3 = generalization.generalize(input, 3);
        assertEquals("123456", level3);
        String level4 = generalization.generalize(input, 4);
        assertEquals("12345", level4);
        String level5 = generalization.generalize(input, 5);
        assertEquals("1234", level5);
    }

    @Test
    public void stringGeneralizationTest2() {
        StringGeneralizer generalization = new StringGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        String input = "1234";
        String level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        String level1 = generalization.generalize(input, 1);
        assertEquals("123", level1);
        String level2 = generalization.generalize(input, 2);
        assertEquals("12", level2);
        String level3 = generalization.generalize(input, 3);
        assertEquals("1", level3);
        String level4 = generalization.generalize(input, 4);
        assertEquals("*", level4);
        String level5 = generalization.generalize(input, 5);
        assertEquals("*", level5);
    }

    @Test(expected = MaskRuntimeException.class)
    public void longGeneralizationInvalidHierarchyLevel1() {
        LongGeneralizer generalization = new LongGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize(1234L, -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void longGeneralizationInvalidHierarchyLevel2() {
        LongGeneralizer generalization = new LongGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize(1234L, 6);
    }

    @Test
    public void longGeneralizationTest1() {
        LongGeneralizer generalization = new LongGeneralizer(5, 10);
        assertEquals(5, generalization.getRootLevel());
        long input = 123456789;
        long level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        long level1 = generalization.generalize(input, 1);
        assertEquals(123456780, level1);
        long level2 = generalization.generalize(input, 2);
        assertEquals(123456700, level2);
        long level3 = generalization.generalize(input, 3);
        assertEquals(123456000, level3);
        long level4 = generalization.generalize(input, 4);
        assertEquals(123450000, level4);
        long level5 = generalization.generalize(input, 5);
        assertEquals(123400000, level5);
    }

    @Test
    public void longGeneralizationTest2() {
        LongGeneralizer generalization = new LongGeneralizer(5, 10);
        assertEquals(5, generalization.getRootLevel());
        long input = 1234L;
        long level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        long level1 = generalization.generalize(input, 1);
        assertEquals(1230, level1);
        long level2 = generalization.generalize(input, 2);
        assertEquals(1200, level2);
        long level3 = generalization.generalize(input, 3);
        assertEquals(1000, level3);
        long level4 = generalization.generalize(input, 4);
        assertEquals(0, level4);
        long level5 = generalization.generalize(input, 5);
        assertEquals(0, level5);
    }

    @Test(expected = MaskRuntimeException.class)
    public void intGeneralizationInvalidHierarchyLevel1() {
        IntGeneralizer generalization = new IntGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize(1234, -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void intGeneralizationInvalidHierarchyLevel2() {
        IntGeneralizer generalization = new IntGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize(1234, 6);
    }

    @Test
    public void intGeneralizationTest1() {
        IntGeneralizer generalization = new IntGeneralizer(5, 10);
        assertEquals(5, generalization.getRootLevel());
        int input = 123456789;
        int level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        int level1 = generalization.generalize(input, 1);
        assertEquals(123456780, level1);
        int level2 = generalization.generalize(input, 2);
        assertEquals(123456700, level2);
        int level3 = generalization.generalize(input, 3);
        assertEquals(123456000, level3);
        int level4 = generalization.generalize(input, 4);
        assertEquals(123450000, level4);
        int level5 = generalization.generalize(input, 5);
        assertEquals(123400000, level5);
    }

    @Test
    public void intGeneralizationTest2() {
        IntGeneralizer generalization = new IntGeneralizer(5, 10);
        assertEquals(5, generalization.getRootLevel());
        int input = 1234;
        int level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        int level1 = generalization.generalize(input, 1);
        assertEquals(1230, level1);
        int level2 = generalization.generalize(input, 2);
        assertEquals(1200, level2);
        int level3 = generalization.generalize(input, 3);
        assertEquals(1000, level3);
        int level4 = generalization.generalize(input, 4);
        assertEquals(0, level4);
        int level5 = generalization.generalize(input, 5);
        assertEquals(0, level5);
    }

    @Test(expected = MaskRuntimeException.class)
    public void shortGeneralizationInvalidHierarchyLevel1() {
        ShortGeneralizer generalization = new ShortGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize((short)1234, -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void shortGeneralizationInvalidHierarchyLevel2() {
        ShortGeneralizer generalization = new ShortGeneralizer(5);
        assertEquals(5, generalization.getRootLevel());
        generalization.generalize((short)1234, 6);
    }

    @Test
    public void shortGeneralizationTest1() {
        ShortGeneralizer generalization = new ShortGeneralizer(3, 10);
        assertEquals(3, generalization.getRootLevel());
        short input = 12345;
        short level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        short level1 = generalization.generalize(input, 1);
        assertEquals(12340, level1);
        short level2 = generalization.generalize(input, 2);
        assertEquals(12300, level2);
        short level3 = generalization.generalize(input, 3);
        assertEquals(12000, level3);
    }

    @Test
    public void shortGeneralizationTest2() {
        ShortGeneralizer generalization = new ShortGeneralizer(3, 10);
        assertEquals(3, generalization.getRootLevel());
        short input = 12;
        short level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        int level1 = generalization.generalize(input, 1);
        assertEquals(10, level1);
        int level2 = generalization.generalize(input, 2);
        assertEquals(0, level2);
        int level3 = generalization.generalize(input, 3);
        assertEquals(0, level3);
    }

    @Test(expected = MaskRuntimeException.class)
    public void byteGeneralizationInvalidHierarchyLevel1() {
        ByteGeneralizer generalization = new ByteGeneralizer(3);
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize((byte)12, -1);
    }

    @Test(expected = MaskRuntimeException.class)
    public void bytesGeneralizationInvalidHierarchyLevel2() {
        ByteGeneralizer generalization = new ByteGeneralizer(3);
        assertEquals(3, generalization.getRootLevel());
        generalization.generalize((byte)12, 6);
    }

    @Test
    public void byteGeneralizationTest1() {
        ByteGeneralizer generalization = new ByteGeneralizer(5, 2);
        assertEquals(5, generalization.getRootLevel());
        byte input = 79;
        byte level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        byte level1 = generalization.generalize(input, 1);
        assertEquals(78, level1);
        byte level2 = generalization.generalize(input, 2);
        assertEquals(76, level2);
        byte level3 = generalization.generalize(input, 3);
        assertEquals(72, level3);
        byte level4 = generalization.generalize(input, 4);
        assertEquals(64, level4);
        byte level5 = generalization.generalize(input, 5);
        assertEquals(64, level5);
    }

    @Test
    public void byteGeneralizationTest2() {
        ByteGeneralizer generalization = new ByteGeneralizer(5, 2);
        assertEquals(5, generalization.getRootLevel());
        byte input = 12;
        byte level0 = generalization.generalize(input, 0);
        assertEquals(input, level0);
        byte level1 = generalization.generalize(input, 1);
        assertEquals(12, level1);
        byte level2 = generalization.generalize(input, 2);
        assertEquals(12, level2);
        byte level3 = generalization.generalize(input, 3);
        assertEquals(8, level3);
        byte level4 = generalization.generalize(input, 4);
        assertEquals(0, level4);
        byte level5 = generalization.generalize(input, 5);
        assertEquals(0, level5);
    }

    @Test
    public void testDoubleGeneralization() {
        System.out.println(2.6 % 1.2);
    }
}
