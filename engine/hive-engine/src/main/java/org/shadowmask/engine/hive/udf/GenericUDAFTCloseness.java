/**
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
package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import scala.Int;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * GenericUDAFTCloseness.
 */
public class GenericUDAFTCloseness extends AbstractGenericUDAFResolver {
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
          throws SemanticException {
    if(parameters.length == 0) {
      throw new UDFArgumentException("Arguments expected");
    }

    for(int i = 0; i < parameters.length; i++) {
      if(parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException("Only primitive type arguments are accepted");
      }
    }

    if (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory()
            != PrimitiveObjectInspector.PrimitiveCategory.INT) {
      throw new UDFArgumentException("The first argument type should be INT");
    }

    return new GenericUDAFTClosenessEvaluator();
  }


  public static class GenericUDAFTClosenessEvaluator extends GenericUDAFEvaluator {
    private IntWritable result;
    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;

    /** init not completed */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
            throws HiveException {
      super.init(m, parameters);
      /*
       * In partial1, parameters are the inspectors of resultant columns
       * produced by a sql.
       */
      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        /*return ObjectInspectorFactory.getReflectionObjectInspector(FreqTable.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
                */
        inputKeyOI = (JavaStringObjectInspector)
                ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                        ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(
                UDAFTCObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        return ObjectInspectorFactory.getStandardMapObjectInspector(
                ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI), inputValueOI);

      } else {
        if(m == Mode.COMPLETE) {
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
          if (parameters.length > 1) throw new UDFArgumentException("Init parameters are incorrect");

          if (!(parameters[0] instanceof StandardMapObjectInspector)) {
            throw new UDFArgumentException("Init error for merge: Parameter is not a standard map object inspector!");
          }

          internalMergeOI = (StandardMapObjectInspector) parameters[0];

          return ObjectInspectorFactory.getReflectionObjectInspector(Statistics.class,
                  ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }
    

    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      HashMap<String, UDAFTCObject> freqMap;
      UDAFTCObject total;

      void put(Object[] values) {
        Integer key_columns_num = ((IntWritable) values[0]).get();
        StringBuilder key_str = new StringBuilder();
        StringBuilder value_str = new StringBuilder();
        for (int i = 1; i <= key_columns_num; i++) {
          key_str.append(values[i]);
        }

        for (int i = key_columns_num + 1; i < values.length; i++) {
          value_str.append(values[i]);
        }

        // generate an unique identifier for one row
        UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(key_str.toString()));
        String key = txt.toString();

        txt = uid.evaluate(new Text(value_str.toString()));
        String value = txt.toString();

        key = key_str.toString();
        value = value_str.toString();

        // add new item to freqMap
        UDAFTCObject sri = new UDAFTCObject(key, value);
        UDAFTCObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          v.put(sri.getSensitiveValue());
          v.increase(1);
        }

        // add new item to total
        total.put(sri.getSensitiveValue());
        total.increase(1);
      }


      /**
       * return the statistics of T-Closeness value in map
       */
      Statistics calculateStatistics() {
        double min = Double.MAX_VALUE;
        double max = 0;
        double mean = 0;
        if (freqMap == null || freqMap.size() == 0) {
          return null;
        }

        ArrayList<Double> equalClass = new ArrayList<Double>();
        for (Map.Entry<String, UDAFTCObject> eachEqualClass : freqMap.entrySet()) {
          double sum = 0;
          Map<String, Integer> eachDeversity = eachEqualClass.getValue().getDeversities();
          for (Map.Entry<String, Integer> sensitiveAttribute : total.getDeversities().entrySet()) {
            String sensitiveAttributeValue = sensitiveAttribute.getKey();
            double q = (double)sensitiveAttribute.getValue()/total.getCount();
            double p = 0;
            Integer pNum = eachDeversity.get(sensitiveAttributeValue);
            if (pNum != null) {
              p = (double)pNum/eachEqualClass.getValue().getCount();
            }
            sum += Math.abs(p - q);
          }
          equalClass.add(sum/2);
        }
        // find max, min and mean
        for (Double e : equalClass) {
          min = e < min ? e : min;
          max = e > max ? e : max;
          mean += e;
        }
        mean /= equalClass.size();

        Statistics result = new Statistics(max, min, mean);

        return result;
      }

    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      FreqTable buffer = new FreqTable();
      reset(buffer);
      return buffer;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFTCObject>();
      ((FreqTable) agg).total = new UDAFTCObject("*");
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) method.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFTCObject> ret = new HashMap<String, UDAFTCObject>();
      ret.putAll(ft.freqMap);
      ft.freqMap.clear();

      /*UDAFTCObject total = new UDAFTCObject("*");
      FreqTable ret = new FreqTable();
      ret.total = total;
      ret.freqMap = freqMap;*/

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Map.Entry<Object, Object> e : result.entrySet()) {

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        Text valueTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(2);
        String value = valueTxt.toString();

        LazyBinaryMap lbm = (LazyBinaryMap)((LazyBinaryStruct)e.getValue()).getField(3);
        Map m = lbm.getMap();

        // merge all the patial maps
        UDAFTCObject v = ft.freqMap.get(row);
        if(v == null) {
          v = new UDAFTCObject(row);
          ft.freqMap.put(row, v);
        }
        for(Object entry : m.entrySet()) {
          String val = ((Map.Entry)entry).getKey().toString();
          int valNumber = ((IntWritable)((Map.Entry)entry).getValue()).get();
          v.put(val, valNumber);
          ft.total.put(val, valNumber);
        }
        v.increase(count.get());
        ft.total.increase(count.get());
      }

    }

    // The return class type to store the statistics of L-Deversity
    static class Statistics {
      public double maxTClosenessValue = 0;
      public double minTClosenessValue = 0;
      public double meanTClosenessValue = 0;

      public Statistics () {
        maxTClosenessValue = 0;
        minTClosenessValue = 0;
        meanTClosenessValue = 0;
      }

      public Statistics (double maxTClosenessValue, double minTClosenessValue, double meanTClosenessValue) {
        this.maxTClosenessValue = maxTClosenessValue;
        this.minTClosenessValue = minTClosenessValue;
        this.meanTClosenessValue = meanTClosenessValue;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return ((FreqTable)agg).calculateStatistics();
    }
  }
}
