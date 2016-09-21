package org.shadowmask.engine.hive.udf;

import java.lang.StringBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryMap;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.UDAFLDObject;
//import org.shadowmask.engine.hive.udf.UDFUIdentifier;

/**
 * Created by root on 7/19/16.
 */
public class GenericUDAFLDeversity extends AbstractGenericUDAFResolver {

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

    return new GenericUDAFLDeversityEvaluator();
  }


  public static class GenericUDAFLDeversityEvaluator extends GenericUDAFEvaluator {
    private IntWritable result;

    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;
    private StandardMapObjectInspector mergeOI;

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
        inputKeyOI = (JavaStringObjectInspector)
            ObjectInspectorFactory.getReflectionObjectInspector(String.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(
            UDAFLDObject.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
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

          inputKeyOI = ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getMapKeyObjectInspector());
          inputValueOI = internalMergeOI.getMapValueObjectInspector();

          mergeOI = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);

          return ObjectInspectorFactory.getReflectionObjectInspector(UDAFLDObject.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }

    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      HashMap<String, UDAFLDObject> freqMap;

      void put(Object[] values) {
        Integer key_columns_num = ((IntWritable) values[0]).get();
        StringBuilder key_str = new StringBuilder();
        StringBuilder value_str = new StringBuilder();
        for (int i = 1; i <= key_columns_num; i++) {
          key_str.append(values[i]);
        }

        for (int i = key_columns_num + 1; i < values.length - 1; i++) {
          value_str.append(values[i]);
        }

        // generate an unique identifier for one row
        UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(key_str.toString()));
        String key = txt.toString();

        txt = uid.evaluate(new Text(value_str.toString()));
        String value = txt.toString();

        UDAFLDObject sri = new UDAFLDObject(key, value);
        UDAFLDObject v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          // increase deversities_ or not
          HashMap d = v.getDeversities();
          if (d.get(sri.getSensitiveValue()) == null) {
            d.put(sri.getSensitiveValue(), 1);
          } else {
            d.putAll(sri.getDeversities());
          }
        }
      }

      /**
       * return the last minimal frequent value in map
       */
      UDAFLDObject min() {
        int min = Integer.MAX_VALUE;
        if(freqMap.size() == 0) {
          return null;
        }
        UDAFLDObject result = null;
        for(UDAFLDObject value : freqMap.values()) {
          if(value.getCount() < min) {
            min = value.getCount();
            result = value;
          }
        }
        return result == null ? null : result;
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
      ((FreqTable) agg).freqMap = new HashMap<String, UDAFLDObject>();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      // TODO: Directly call merge(...) method.
      ((FreqTable) agg).put(parameters);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      FreqTable ft = (FreqTable) agg;
      HashMap<String, UDAFLDObject> ret = new HashMap<String, UDAFLDObject>();
      ret.putAll(ft.freqMap);
      ft.freqMap.clear();

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Entry<Object, Object> e : result.entrySet()) {

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        Text valueTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(2);
        String value = valueTxt.toString();

        LazyBinaryMap lbm = (LazyBinaryMap)((LazyBinaryStruct)e.getValue()).getField(3);
        Map m = lbm.getMap();

        UDAFLDObject sri = new UDAFLDObject(row, value);
        sri.setCount(count.get());

        // merge all the patial maps
        UDAFLDObject v = ft.freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          ft.freqMap.put(sri.getRow(), sri);
        } else {
          // increase deversities_ or not
          HashMap d = v.getDeversities();
          if (d.get(sri.getSensitiveValue()) == null) {
            // TODO: Currently, always set the count of sensitive columns
            // as 1, because it's easy to get L value for a kind of class
            // through the size the corresponding hash map.
            d.put(sri.getSensitiveValue(), 1);
          } else {
            d.putAll(sri.getDeversities());
          }
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      UDAFLDObject minValue = ((FreqTable)agg).min();
      if(minValue == null) {
        return null;
      } else {
        return minValue;
      }
    }
  }

}
