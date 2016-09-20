package org.shadowmask.engine.hive.udf;

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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ReflectionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Created by root on 7/19/16.
 */
public class GenericUDAFKAnonymityString extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
          throws SemanticException {
    if(parameters.length == 0) {
      throw new UDFArgumentException("Argument expected");
    }
    for(int i = 0; i < parameters.length; i++) {
      if(parameters[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentException("Only primitive type arguments are accepted");
      }
    }
    return new GenericUDAFKAnonymityEvaluator();
  }


  public static class GenericUDAFKAnonymityEvaluator extends GenericUDAFEvaluator {
    private IntWritable result;

    private ObjectInspector inputKeyOI;
    private ObjectInspector inputValueOI;

    private StandardMapObjectInspector internalMergeOI;
    private StandardMapObjectInspector mergeOI;

    static class StringRowInfo {
      private String row_;
      private int count_ = 0;

      public StringRowInfo(String code) {
        count_ = 1;
        row_ = code;
      }

      @Override
      public int hashCode() {
        int hash = 0;
        hash = row_.hashCode();
        return hash;
      }

      @Override
      public boolean equals(Object obj) {
        if(this == obj) {
          return true;
        }
        if(obj == null || getClass() != obj.getClass()) {
          return false;
        }
        StringRowInfo row = (StringRowInfo) obj;
        if(!row_.equals(row.row_)) return false;
        return true;
      }

      public String getRow() {
        return row_;
      }

      public Integer getCount() {
        return count_;
      }

      void setCount(Integer count) {
        this.count_ = count;
      }

      public void increase(Integer cnt) {
        this.count_ += cnt;
      }

    }

    /** init not completed */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      System.out.println("init " + m);
      /*
       * In partial1, parameters are the inspectors of resultant columns produced by a sql.
       */

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        System.out.println("partial1 running...");
        inputKeyOI =
            //(JavaIntObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(Integer.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            (JavaStringObjectInspector) ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        System.out.println("inputKeyOI ==> " + inputKeyOI);

        inputValueOI = ObjectInspectorFactory.getReflectionObjectInspector(StringRowInfo.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        System.out.println("inputValueOI ==> " + inputValueOI);
        return ObjectInspectorFactory.getStandardMapObjectInspector(ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI),
            inputValueOI);
      } else {
        //if(m == Mode.FINAL) {
        //    System.out.println("final running...");
        //    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        //}
        if(m == Mode.COMPLETE) {
          System.out.println("complete running...");
          return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
        } else {
          System.out.println("partial2 or merge running...");
          if (parameters.length > 1) System.out.println("init parameters are incorrect...");
          if (!(parameters[0] instanceof StandardMapObjectInspector)) {
            System.out.println("mege init error......");
          }

          System.out.println("parameter size is : " + parameters.length);
          System.out.println("parameter[0] is : " + parameters[0]);
          internalMergeOI = (StandardMapObjectInspector) parameters[0];

          inputKeyOI = ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getMapKeyObjectInspector());
          System.out.println("inputKeyOI is : " + inputKeyOI);
          inputValueOI = internalMergeOI.getMapValueObjectInspector();
          //inputValueOI = ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI.getMapValueObjectInspector());
          System.out.println("inputValueOI is : " + inputValueOI);

          mergeOI = (StandardMapObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
              
          return ObjectInspectorFactory.getReflectionObjectInspector(StringRowInfo.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        }
      }
    }

    /** class for storing frequency of different inputs. */
    @AggregationType
    static class FreqTable extends AbstractAggregationBuffer {
      //HashMap<Integer, StringRowInfo> freqMap;
      HashMap<String, StringRowInfo> freqMap;

      void put(Object[] values) {
        String str = "";
        for (Object o : values) {
          str += o;
        }

        // generate an unique identifier for one row
        UDFUIdentifier uid = new UDFUIdentifier();
        Text txt = uid.evaluate(new Text(str));
        String code = txt.toString();

        StringRowInfo sri = new StringRowInfo(code);
        StringRowInfo v = freqMap.get(sri.getRow());
        if(v == null) {
          sri.setCount(1);
          freqMap.put(sri.getRow(), sri);
        } else {
          sri.increase(v.getCount());
          freqMap.put(sri.getRow(), sri);
        }
      }

      /**
       * return the last minimal frequent value in map
       */
      StringRowInfo min() {
        int min = Integer.MAX_VALUE;
        if(freqMap.size() == 0) {
          return null;
        }
        StringRowInfo result = null;
        System.out.println("freqMap size in min() : " + freqMap.size());
        for(StringRowInfo value : freqMap.values()) {
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
      //((FreqTable) agg).freqMap = new HashMap<Integer, StringRowInfo>();
      ((FreqTable) agg).freqMap = new HashMap<String, StringRowInfo>();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      System.out.println("iterate running : " + parameters.length);

      ((FreqTable) agg).put(parameters);
      for(int i = 0; i < parameters.length; i++) {
        System.out.println("iterate : " + parameters[i]);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      System.out.println("terminatePartial running : start casting!!!!");
      FreqTable ft = (FreqTable) agg;
      //HashMap<Integer, StringRowInfo> ret = new HashMap<Integer, StringRowInfo>(ft.freqMap);
      HashMap<String, StringRowInfo> ret = new HashMap<String, StringRowInfo>(ft.freqMap);

      System.out.println("terminatePartial running : origin size " + ((FreqTable) agg).freqMap.size());
      System.out.println("terninatePartial running : result size " + ret.size() + "!!!!");

      return ret;
    }

    /** NOTE: LazyBinaryMap's key object must be a wrtiable primitive objects*/
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      System.out.println("merge1");
      System.out.println("partial is " + partial);
      //HashMap<Integer, RowInfo> result = (HashMap<Integer, RowInfo>) internalMergeOI.getMap(partial);
      HashMap<Object, Object> result = (HashMap<Object, Object>) internalMergeOI.getMap(partial);
      FreqTable ft = (FreqTable) agg;

      for (Entry<Object, Object> e : result.entrySet()) {
        System.out.println("values : " + e.getValue());
        System.out.println("values 0 : " + ((LazyBinaryStruct)e.getValue()).getField(0));
        System.out.println("values 1 : " + ((LazyBinaryStruct)e.getValue()).getField(1));

        Text rowTxt = (Text)((LazyBinaryStruct)e.getValue()).getField(0);
        String row = rowTxt.toString();
        IntWritable count = (IntWritable)((LazyBinaryStruct)e.getValue()).getField(1);

        System.out.println("inputValueOI is " + inputValueOI);

        StringRowInfo sri = new StringRowInfo(row);
        sri.setCount(count.get());

        // merge all the patial maps
        if (ft.freqMap.containsKey(sri.getRow())) {
            StringRowInfo base = ft.freqMap.get(sri.getRow());
            sri.increase(base.getCount());
            ft.freqMap.put(sri.getRow(), sri);
        } else {
            ft.freqMap.put(sri.getRow(), sri);
        }
      }

      System.out.println("merge3:"+((FreqTable)agg).freqMap.size());
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      System.out.println("terminate");
      StringRowInfo minValue = ((FreqTable)agg).min();
      if(minValue == null) {
        System.out.println("terminate: null");
        return null;
      } else {
        //Integer result = new IntWritable(minValue);
        ArrayList<StringRowInfo> result = new ArrayList<StringRowInfo>();
        System.out.println("terminate:" + ((FreqTable) agg).freqMap.size());

        //ObjectInspector oi = ObjectInspectorFactory.getReflectionObjectInspector(StringRowInfo.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        //System.out.println(oi);
        //ArrayList<String> fieldNames = new ArrayList();
        //fieldNames.add("row_");
        //fieldNames.add("count_");
        //ArrayList<ObjectInspector> fields = new ArrayList();
        //fields.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        //fields.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
        //StandardStructObjectInspector ssoi = ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fields);
        //Object ret = ObjectInspectorUtils.copyToStandardObject(minValue, ssoi);
        
        //result.add((StringRowInfo) ret);
        //return ret;
        return minValue;
      }
    }
  }
}
