package org.shadowmask.engine.hive.udf;

import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Random;
import javax.crypto.*;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testUDFAscii() {

    }

    public void testUDFGeneralization() {
        System.out.println("----------- UDFGeneralization Test ------------");
        UDFGeneralization test = new UDFGeneralization();

        //data = integer
        IntWritable val1;
        // mode=floor, data=integer
        int[] data1={Integer.MIN_VALUE, Integer.MIN_VALUE+5, 100, 0, -14, 9, Integer.MAX_VALUE, Integer.MAX_VALUE-5};
        int[] unit1={10,5};
        int mode1 = 0;
        int[] result1={Integer.MIN_VALUE, Integer.MIN_VALUE, 100, 0, -20, 0, 2147483640, 2147483640,
                Integer.MIN_VALUE, -2147483645, 100, 0, -15, 5, 2147483645, 2147483640}; //expected result
        for (int i = 0, k = 0; i < unit1.length; i++) {
            for(int j = 0; j < data1.length; j++) {
                IntWritable data = new IntWritable(data1[j]);
                IntWritable unit = new IntWritable(unit1[i]);
                IntWritable mode = new IntWritable(mode1);
                val1 = test.evaluate(data, mode, unit);
                if (val1.get() != result1[k]) {
                    System.out.printf("Int failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data1[j], unit1[i], result1[k], val1.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=integer
        int[] data2={Integer.MIN_VALUE, Integer.MIN_VALUE+5, 100, 0, -14, 9, Integer.MAX_VALUE, Integer.MAX_VALUE-5};
        int[] unit2={10,5};
        int mode2 = 1;
        int[] result2={-2147483640, -2147483640, 100, 0, -10, 10, Integer.MAX_VALUE, Integer.MAX_VALUE,
                -2147483645, -2147483640, 100, 0, -10, 10, Integer.MAX_VALUE, 2147483645}; //expected result
        for (int i = 0, k = 0; i < unit2.length; i++) {
            for(int j = 0; j < data2.length; j++) {
                IntWritable data = new IntWritable(data2[j]);
                IntWritable unit = new IntWritable(unit2[i]);
                IntWritable mode = new IntWritable(mode2);
                val1 = test.evaluate(data, mode, unit);
                if (val1.get() != result2[k]) {
                    System.out.printf("Int failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data2[i], unit2[i], result2[i], val1.get());
                }
                k ++;
            }
        }

        //data = double
        DoubleWritable val2;
        // mode=floor, data=double
        double[] data3={0.1,0,-0.1,9.9,10,10.01,-10.3,-9.98,-10,-0.002,23.5,200.45,32.4,-0.3,-34.5};
        int[] unit3={10,10,10,10,10,10,10,10,10,10,6,20,1,1,1};
        double[] result3={0,0,-10,0,10,10,-20,-10,-10,-10,18,200,32,-1,-35};
        for (int i = 0; i < data3.length; i++) {
            DoubleWritable data = new DoubleWritable(data3[i]);
            IntWritable unit = new IntWritable(unit3[i]);
            IntWritable mode = new IntWritable(mode1);
            val2 = test.evaluate(data,mode,unit);
            if (val2.get() != result3[i]) {
                System.out.printf("Double failed : data=%f,mode=0,unit=%d,expect result=%f,result=%f\n",data3[i],unit3[i],result3[i],val2.get());
            }
        }

        // mode=ceil, data=double
        double[] data4={0.1,0,-0.1,9.9,10,10.01,-10.3,-9.98,-10,-0.002,23.5,200.45,32.4,-0.3,-34.5};
        int[] unit4={10,10,10,10,10,10,10,10,10,10,6,20,1,1,1};
        double[] result4={10,0,0,10,10,20,-10,0,-10,0,24,220,33,0,-34};
        for (int i = 0; i < data4.length; i++) {
            DoubleWritable data = new DoubleWritable(data4[i]);
            IntWritable unit = new IntWritable(unit4[i]);
            IntWritable mode = new IntWritable(mode2);
            val2 = test.evaluate(data,mode,unit);
            if (val2.get() != result4[i]) {
                System.out.printf("Double failed : data=%f,mode=1,unit=%d,expect result=%f,result=%f\n",data4[i],unit4[i],result4[i],val2.get());
            }
        }

        //data = float
        FloatWritable val3;
        // mode=floor, data=float
        float[] data5={0.1f,0,-0.1f,9.9f,10,10.01f,-10.3f,-9.98f,-10,-0.002f,23.5f,200.45f,32.4f,-0.3f,-34.5f};
        int[] unit5={10,10,10,10,10,10,10,10,10,10,6,20,1,1,1};
        double[] result5={0,0,-10,0,10,10,-20,-10,-10,-10,18,200,32,-1,-35};
        for (int i = 0; i < data5.length; i++) {
            FloatWritable data = new FloatWritable(data5[i]);
            IntWritable unit = new IntWritable(unit5[i]);
            IntWritable mode = new IntWritable(mode1);
            val3 = test.evaluate(data,mode,unit);
            if (val3.get() != result5[i]) {
                System.out.printf("Float failed : data=%f,mode=0,unit=%d,expect result=%f,result=%f\n",data5[i],unit5[i],result5[i],val3.get());
            }
        }

        // mode=ceil, data=float
        float[] data6={0.1f,0,-0.1f,9.9f,10,10.01f,-10.3f,-9.98f,-10,-0.002f,23.5f,200.45f,32.4f,-0.3f,-34.5f};
        int[] unit6={10,10,10,10,10,10,10,10,10,10,6,20,1,1,1};
        double[] result6={10,0,0,10,10,20,-10,0,-10,0,24,220,33,0,-34};
        for (int i = 0; i < data6.length; i++) {
            FloatWritable data = new FloatWritable(data6[i]);
            IntWritable unit = new IntWritable(unit6[i]);
            IntWritable mode = new IntWritable(mode2);
            val3 = test.evaluate(data,mode,unit);
            if (val3.get() != result6[i]) {
                System.out.printf("Float failed : data=%f,mode=1,unit=%d,expect result=%f,result=%f\n",data6[i],unit6[i],result6[i],val3.get());
            }
        }

        //data = long
        LongWritable val4;
        // mode=floor, data=long
        long[] data7={Long.MIN_VALUE, Long.MIN_VALUE+5, 100, 0, -14, 9, Long.MAX_VALUE, Long.MAX_VALUE-5};
        int[] unit7={10,5};
        long[] result7={Long.MIN_VALUE, Long.MIN_VALUE, 100, 0, -20, 0, 9223372036854775800L, 9223372036854775800L,
                Long.MIN_VALUE, -9223372036854775805L, 100, 0, -15, 5, 9223372036854775805L, 9223372036854775800L}; //expected result
        for (int i = 0, k = 0; i < unit7.length; i++) {
            for(int j = 0; j < data7.length; j++) {
                LongWritable data = new LongWritable(data7[j]);
                IntWritable unit = new IntWritable(unit7[i]);
                IntWritable mode = new IntWritable(mode1);
                val4 = test.evaluate(data, mode, unit);
                if (val4.get() != result7[k]) {
                    System.out.printf("Long failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data7[j], unit7[i], result7[k], val4.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=long
        long[] data8={Long.MIN_VALUE, Long.MIN_VALUE+5, 100, 0, -14, 9, Long.MAX_VALUE, Long.MAX_VALUE-5};
        int[] unit8={10,5};
        long[] result8={-9223372036854775800L, -9223372036854775800L, 100, 0, -10, 10, Long.MAX_VALUE, Long.MAX_VALUE,
                -9223372036854775805L, -9223372036854775800L, 100, 0, -10, 10, Long.MAX_VALUE, 9223372036854775805L}; //expected result
        for (int i = 0, k = 0; i < unit8.length; i++) {
            for(int j = 0; j < data8.length; j++) {
                LongWritable data = new LongWritable(data8[j]);
                IntWritable unit = new IntWritable(unit8[i]);
                IntWritable mode = new IntWritable(mode2);
                val4 = test.evaluate(data, mode, unit);
                if (val4.get() != result8[k]) {
                    System.out.printf("Long failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data8[i], unit8[i], result8[i], val4.get());
                }
                k ++;
            }
        }

        //data = short
        ShortWritable val5;
        // mode=floor, data=short
        short[] data9={Short.MIN_VALUE, Short.MIN_VALUE+5, 100, 0, -14, 9, Short.MAX_VALUE, Short.MAX_VALUE-5};
        int[] unit9={10,5};
        short[] result9={Short.MIN_VALUE, Short.MIN_VALUE, 100, 0, -20, 0, 32760, 32760,
                Short.MIN_VALUE, -32765, 100, 0, -15, 5, 32765, 32760}; //expected result
        for (int i = 0, k = 0; i < unit9.length; i++) {
            for(int j = 0; j < data9.length; j++) {
                ShortWritable data = new ShortWritable(data9[j]);
                IntWritable unit = new IntWritable(unit9[i]);
                IntWritable mode = new IntWritable(mode1);
                val5 = test.evaluate(data, mode, unit);
                if (val5.get() != result9[k]) {
                    System.out.printf("Short failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data9[j], unit9[i], result9[k], val5.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=short
        short[] data10={Short.MIN_VALUE, Short.MIN_VALUE+5, 100, 0, -14, 9, Short.MAX_VALUE, Short.MAX_VALUE-5};
        int[] unit10={10,5};
        short[] result10={-32760, -32760, 100, 0, -10, 10, Short.MAX_VALUE, Short.MAX_VALUE,
                -32765, -32760, 100, 0, -10, 10, Short.MAX_VALUE, 32765}; //expected result
        for (int i = 0, k = 0; i < unit10.length; i++) {
            for(int j = 0; j < data10.length; j++) {
                ShortWritable data = new ShortWritable(data10[j]);
                IntWritable unit = new IntWritable(unit10[i]);
                IntWritable mode = new IntWritable(mode2);
                val5 = test.evaluate(data, mode, unit);
                if (val5.get() != result10[k]) {
                    System.out.printf("Short failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data10[i], unit10[i], result10[i], val5.get());
                }
                k ++;
            }
        }

        //data = byte
        ByteWritable val6;
        // mode=floor, data=byte
        byte[] data11={Byte.MIN_VALUE, Byte.MIN_VALUE+5, 100, 0, -14, 9, Byte.MAX_VALUE, Byte.MAX_VALUE-5};
        int[] unit11={10,5};
        byte[] result11={Byte.MIN_VALUE, Byte.MIN_VALUE, 100, 0, -20, 0, 120, 120,
                Byte.MIN_VALUE, -125, 100, 0, -15, 5, 125, 120}; //expected result
        for (int i = 0, k = 0; i < unit11.length; i++) {
            for(int j = 0; j < data9.length; j++) {
                ByteWritable data = new ByteWritable(data11[j]);
                IntWritable unit = new IntWritable(unit11[i]);
                IntWritable mode = new IntWritable(mode1);
                val6 = test.evaluate(data, mode, unit);
                if (val6.get() != result11[k]) {
                    System.out.printf("Byte failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data11[j], unit11[i], result11[k], val6.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=byte
        byte[] data12={Byte.MIN_VALUE, Byte.MIN_VALUE+5, 100, 0, -14, 9, Byte.MAX_VALUE, Byte.MAX_VALUE-5};
        int[] unit12={10,5};
        byte[] result12={-120, -120, 100, 0, -10, 10, Byte.MAX_VALUE, Byte.MAX_VALUE,
                -125, -120, 100, 0, -10, 10, Byte.MAX_VALUE, 125}; //expected result
        for (int i = 0, k = 0; i < unit12.length; i++) {
            for(int j = 0; j < data12.length; j++) {
                ByteWritable data = new ByteWritable(data12[j]);
                IntWritable unit = new IntWritable(unit12[i]);
                IntWritable mode = new IntWritable(mode2);
                val6 = test.evaluate(data, mode, unit);
                if (val6.get() != result12[k]) {
                    System.out.printf("Byte failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data12[i], unit12[i], result12[i], val6.get());
                }
                k ++;
            }
        }
    }

    public void testUDFAge() {
        System.out.println("----------- UDFAge Test ------------");
        UDFAge test = new UDFAge();

        //data = integer
        IntWritable val1;
        // mode=floor, data=integer
        int[] data1={Integer.MIN_VALUE, Integer.MIN_VALUE+5, 100, 0, -14, 9, Integer.MAX_VALUE, Integer.MAX_VALUE-5};
        int[] unit1={10,5};
        int mode1 = 0;
        int[] result1={Integer.MIN_VALUE, Integer.MIN_VALUE, 100, 0, -20, 0, 2147483640, 2147483640,
                Integer.MIN_VALUE, -2147483645, 100, 0, -15, 5, 2147483645, 2147483640}; //expected result
        for (int i = 0, k = 0; i < unit1.length; i++) {
            for(int j = 0; j < data1.length; j++) {
                IntWritable data = new IntWritable(data1[j]);
                IntWritable unit = new IntWritable(unit1[i]);
                IntWritable mode = new IntWritable(mode1);
                val1 = test.evaluate(data, mode, unit);
                if (val1.get() != result1[k]) {
                    System.out.printf("Int failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data1[j], unit1[i], result1[k], val1.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=integer
        int[] data2={Integer.MIN_VALUE, Integer.MIN_VALUE+5, 100, 0, -14, 9, Integer.MAX_VALUE, Integer.MAX_VALUE-5};
        int[] unit2={10,5};
        int mode2 = 1;
        int[] result2={-2147483640, -2147483640, 100, 0, -10, 10, Integer.MAX_VALUE, Integer.MAX_VALUE,
                -2147483645, -2147483640, 100, 0, -10, 10, Integer.MAX_VALUE, 2147483645}; //expected result
        for (int i = 0, k = 0; i < unit2.length; i++) {
            for(int j = 0; j < data2.length; j++) {
                IntWritable data = new IntWritable(data2[j]);
                IntWritable unit = new IntWritable(unit2[i]);
                IntWritable mode = new IntWritable(mode2);
                val1 = test.evaluate(data, mode, unit);
                if (val1.get() != result2[k]) {
                    System.out.printf("Int failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data2[i], unit2[i], result2[i], val1.get());
                }
                k ++;
            }
        }

        //data = long
        LongWritable val4;
        // mode=floor, data=long
        long[] data7={Long.MIN_VALUE, Long.MIN_VALUE+5, 100, 0, -14, 9, Long.MAX_VALUE, Long.MAX_VALUE-5};
        int[] unit7={10,5};
        long[] result7={Long.MIN_VALUE, Long.MIN_VALUE, 100, 0, -20, 0, 9223372036854775800L, 9223372036854775800L,
                Long.MIN_VALUE, -9223372036854775805L, 100, 0, -15, 5, 9223372036854775805L, 9223372036854775800L}; //expected result
        for (int i = 0, k = 0; i < unit7.length; i++) {
            for(int j = 0; j < data7.length; j++) {
                LongWritable data = new LongWritable(data7[j]);
                IntWritable unit = new IntWritable(unit7[i]);
                IntWritable mode = new IntWritable(mode1);
                val4 = test.evaluate(data, mode, unit);
                if (val4.get() != result7[k]) {
                    System.out.printf("Long failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data7[j], unit7[i], result7[k], val4.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=long
        long[] data8={Long.MIN_VALUE, Long.MIN_VALUE+5, 100, 0, -14, 9, Long.MAX_VALUE, Long.MAX_VALUE-5};
        int[] unit8={10,5};
        long[] result8={-9223372036854775800L, -9223372036854775800L, 100, 0, -10, 10, Long.MAX_VALUE, Long.MAX_VALUE,
                -9223372036854775805L, -9223372036854775800L, 100, 0, -10, 10, Long.MAX_VALUE, 9223372036854775805L}; //expected result
        for (int i = 0, k = 0; i < unit8.length; i++) {
            for(int j = 0; j < data8.length; j++) {
                LongWritable data = new LongWritable(data8[j]);
                IntWritable unit = new IntWritable(unit8[i]);
                IntWritable mode = new IntWritable(mode2);
                val4 = test.evaluate(data, mode, unit);
                if (val4.get() != result8[k]) {
                    System.out.printf("Long failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data8[i], unit8[i], result8[i], val4.get());
                }
                k ++;
            }
        }

        //data = short
        ShortWritable val5;
        // mode=floor, data=short
        short[] data9={Short.MIN_VALUE, Short.MIN_VALUE+5, 100, 0, -14, 9, Short.MAX_VALUE, Short.MAX_VALUE-5};
        int[] unit9={10,5};
        short[] result9={Short.MIN_VALUE, Short.MIN_VALUE, 100, 0, -20, 0, 32760, 32760,
                Short.MIN_VALUE, -32765, 100, 0, -15, 5, 32765, 32760}; //expected result
        for (int i = 0, k = 0; i < unit9.length; i++) {
            for(int j = 0; j < data9.length; j++) {
                ShortWritable data = new ShortWritable(data9[j]);
                IntWritable unit = new IntWritable(unit9[i]);
                IntWritable mode = new IntWritable(mode1);
                val5 = test.evaluate(data, mode, unit);
                if (val5.get() != result9[k]) {
                    System.out.printf("Short failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data9[j], unit9[i], result9[k], val5.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=short
        short[] data10={Short.MIN_VALUE, Short.MIN_VALUE+5, 100, 0, -14, 9, Short.MAX_VALUE, Short.MAX_VALUE-5};
        int[] unit10={10,5};
        short[] result10={-32760, -32760, 100, 0, -10, 10, Short.MAX_VALUE, Short.MAX_VALUE,
                -32765, -32760, 100, 0, -10, 10, Short.MAX_VALUE, 32765}; //expected result
        for (int i = 0, k = 0; i < unit10.length; i++) {
            for(int j = 0; j < data10.length; j++) {
                ShortWritable data = new ShortWritable(data10[j]);
                IntWritable unit = new IntWritable(unit10[i]);
                IntWritable mode = new IntWritable(mode2);
                val5 = test.evaluate(data, mode, unit);
                if (val5.get() != result10[k]) {
                    System.out.printf("Short failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data10[i], unit10[i], result10[i], val5.get());
                }
                k ++;
            }
        }

        //data = byte
        ByteWritable val6;
        // mode=floor, data=byte
        byte[] data11={Byte.MIN_VALUE, Byte.MIN_VALUE+5, 100, 0, -14, 9, Byte.MAX_VALUE, Byte.MAX_VALUE-5};
        int[] unit11={10,5};
        byte[] result11={Byte.MIN_VALUE, Byte.MIN_VALUE, 100, 0, -20, 0, 120, 120,
                Byte.MIN_VALUE, -125, 100, 0, -15, 5, 125, 120}; //expected result
        for (int i = 0, k = 0; i < unit11.length; i++) {
            for(int j = 0; j < data9.length; j++) {
                ByteWritable data = new ByteWritable(data11[j]);
                IntWritable unit = new IntWritable(unit11[i]);
                IntWritable mode = new IntWritable(mode1);
                val6 = test.evaluate(data, mode, unit);
                if (val6.get() != result11[k]) {
                    System.out.printf("Byte failed : data=%d,mode=0,unit=%d,expect result=%d,result=%d\n", data11[j], unit11[i], result11[k], val6.get());
                }
                k ++;
            }
        }

        // mode=ceil, data=byte
        byte[] data12={Byte.MIN_VALUE, Byte.MIN_VALUE+5, 100, 0, -14, 9, Byte.MAX_VALUE, Byte.MAX_VALUE-5};
        int[] unit12={10,5};
        byte[] result12={-120, -120, 100, 0, -10, 10, Byte.MAX_VALUE, Byte.MAX_VALUE,
                -125, -120, 100, 0, -10, 10, Byte.MAX_VALUE, 125}; //expected result
        for (int i = 0, k = 0; i < unit12.length; i++) {
            for(int j = 0; j < data12.length; j++) {
                ByteWritable data = new ByteWritable(data12[j]);
                IntWritable unit = new IntWritable(unit12[i]);
                IntWritable mode = new IntWritable(mode2);
                val6 = test.evaluate(data, mode, unit);
                if (val6.get() != result12[k]) {
                    System.out.printf("Byte failed : data=%d,mode=1,unit=%d,expect result=%d,result=%d\n", data12[i], unit12[i], result12[i], val6.get());
                }
                k ++;
            }
        }
    }

    public void testUDFTruncation() {
        System.out.println("----------- UDFTruncation Test ------------");
        UDFTruncation test = new UDFTruncation();
        String str = new String("hello! 英特尔@Shanghai紫竹");
        System.out.println("Input String is : \n" + str);
        Text text = new Text(str);
        Text result = new Text();
        int[] length = {0,1,2,5,10,15,20,21,22,25,30};
        int[] mode = {0,1};
        for(int mod : mode) {
            IntWritable m = new IntWritable(mod);
            System.out.println("<mode> = " + mod + " 0 : reserve first half; 1 : reserve second half");
            for (int len : length) {
                IntWritable l = new IntWritable(len);
                result = test.evaluate(text, l, m);

                System.out.println("length = " + len + " : " + result.toString());
            }
        }
    }

    public void testUDFMask() {
        System.out.println("----------- UDFMask Test ------------");
        UDFMask test = new UDFMask();
        String str = new String("hello! 英特尔@Shanghai紫竹");
        System.out.println("Input String is : \n" + str);
        Text text = new Text(str);
        Text result = new Text();
        Text tag = new Text("*");
        int[] begin = {-5,0,1,5,8,13,20,21,22,30};
        int[] end = {-10,-4,0,1,6,10,16,21,22,25,50};
        for(int b : begin) {
            for(int e : end) {
                if(b <= e) {
                    IntWritable beg = new IntWritable(b);
                    IntWritable en = new IntWritable(e);
                    result = test.evaluate(text, beg, en, tag);
                    if (result != null) {
                        System.out.println(result.toString() + ":" + b + "~" + e);
                    } else {
                        System.out.println("null" + ":" + b + "~" + e);
                    }
                }
            }
        }
    }

    public void testUDFShift() {
        System.out.println("----------- UDFShift Test ------------");
        UDFShift test = new UDFShift();

        IntWritable[] direction = new IntWritable[2];
        direction[0] = new IntWritable(0);
        direction[1] = new IntWritable(1);
        IntWritable digit = new IntWritable();
        IntWritable[] mode = new IntWritable[2];
        mode[0] = new IntWritable(0);
        mode[1] = new IntWritable(1);

        // int version
        System.out.println("----int version----");
        int[] dataInt = {1223435564,-1298490000};
        for(int data : dataInt) {
            IntWritable input = new IntWritable(data);
            System.out.println(input.get() + ":" + Integer.toBinaryString(input.get()) + "->" );
            for (IntWritable mod : mode) {
                System.out.println("<mode> = " + mod.get());
                for (IntWritable dir : direction) {
                    System.out.println("<direction> = " + dir.get());
                    for (int i = 0; i <= 31; i += 4) {
                        digit.set(i);
                        IntWritable output = test.evaluate(input, dir, digit, mod);
                        System.out.println(digit.get() + " digits : " + Integer.toBinaryString(output.get()) + ":" + output.get());
                    }
                    digit.set(31);
                    IntWritable output = test.evaluate(input, dir, digit, mod);
                    System.out.println(digit.get() + " digits : " + Integer.toBinaryString(output.get()) + ":" + output.get());
                }
            }
        }

        // long version
        System.out.println("----long version----");
        long[] dataLong = {243866444554000L,-29777545528000L};
        for(long data : dataLong) {
            LongWritable input = new LongWritable(data);
            System.out.println(input.get() + ":" + Long.toBinaryString(input.get()) + "->" );
            for (IntWritable mod : mode) {
                System.out.println("<mode> = " + mod.get());
                for (IntWritable dir : direction) {
                    System.out.println("<direction> = " + dir.get());
                    for (int i = 0; i <= 63; i += 8) {
                        digit.set(i);
                        LongWritable output = test.evaluate(input, dir, digit, mod);
                        System.out.println(digit.get() + " digits : " + Long.toBinaryString(output.get()) + ":" + output.get());
                    }
                    digit.set(63);
                    LongWritable output = test.evaluate(input, dir, digit, mod);
                    System.out.println(digit.get() + " digits : " + Long.toBinaryString(output.get()) + ":" + output.get());
                }
            }
        }

        // short version
        System.out.println("----short version----");
        short[] dataShort = {500,-400};
        for(short data : dataShort) {
            ShortWritable input = new ShortWritable(data);
            System.out.println(input.get() + ":" + Integer.toBinaryString(input.get()) + "->" );
            for (IntWritable mod : mode) {
                System.out.println("<mode> = " + mod.get());
                for (IntWritable dir : direction) {
                    System.out.println("<direction> = " + dir.get());
                    for (int i = 0; i <= 15; i += 2) {
                        digit.set(i);
                        ShortWritable output = test.evaluate(input, dir, digit, mod);
                        String binary = Integer.toBinaryString(output.get());
                        if (binary.length() <= 16) {
                            System.out.println(digit.get() + " digits : " + binary + ":" + output.get());
                        }
                        else {
                            System.out.println(digit.get() + " digits : " + binary.substring(binary.length() - 16) + ":" + output.get());
                        }
                    }
                    digit.set(15);
                    ShortWritable output = test.evaluate(input, dir, digit, mod);
                    String binary = Integer.toBinaryString(output.get());
                    if (binary.length() <= 16) {
                        System.out.println(digit.get() + " digits : " + binary + ":" + output.get());
                    }
                    else {
                        System.out.println(digit.get() + " digits : " + binary.substring(binary.length() - 16) + ":" + output.get());
                    }
                }
            }
        }

        // byte version
        System.out.println("----byte version----");
        byte[] dataByte = {100,-45};
        for(byte data : dataByte) {
            ByteWritable input = new ByteWritable(data);
            System.out.println(input.get() + ":" + Integer.toBinaryString(input.get()) + "->" );
            for (IntWritable mod : mode) {
                System.out.println("<mode> = " + mod.get());
                for (IntWritable dir : direction) {
                    System.out.println("<direction> = " + dir.get());
                    for (int i = 0; i <= 7; i += 1) {
                        digit.set(i);
                        ByteWritable output = test.evaluate(input, dir, digit, mod);
                        String binary = Integer.toBinaryString(output.get());
                        if (binary.length() <= 8) {
                            System.out.println(digit.get() + " digits : " + binary + ":" + output.get());
                        }
                        else {
                            System.out.println(digit.get() + " digits : " + binary.substring(binary.length() - 8) + ":" + output.get());
                        }
                    }
                }
            }
        }

    }

    public void testUDFHiding() {
        System.out.println("----------- UDFHiding Test ------------");
        UDFHiding test = new UDFHiding();

        ByteWritable dataByte = new ByteWritable((byte)34);
        ByteWritable valueByte = new ByteWritable((byte)46);
        if (test.evaluate(dataByte, valueByte).get() != (byte)46) {
            System.out.println("dataByte fails");
        }

        DoubleWritable dataDouble = new DoubleWritable(34.45);
        DoubleWritable valueDouble = new DoubleWritable(4.6);
        if (test.evaluate(dataDouble, valueDouble).get() != 4.6) {
            System.out.println("dataDouble fails");
        }

        FloatWritable dataFloat = new FloatWritable(34.45f);
        FloatWritable valueFloat = new FloatWritable(4.6f);
        if (test.evaluate(dataFloat, valueFloat).get() != 4.6f) {
            System.out.println("dataFloat fails");
        }

        IntWritable dataInt = new IntWritable(33456);
        IntWritable valueInt = new IntWritable(-4546);
        if (test.evaluate(dataInt, valueInt).get() != -4546) {
            System.out.println("dataInt fails");
        }

        LongWritable dataLong = new LongWritable(334563L);
        LongWritable valueLong = new LongWritable(-45465555L);
        if (test.evaluate(dataLong, valueLong).get() != -45465555L) {
            System.out.println("dataLong fails");
        }

        ShortWritable dataShort = new ShortWritable((short)33);
        ShortWritable valueShort = new ShortWritable((short)-4548);
        if (test.evaluate(dataShort, valueShort).get() != (short)-4548) {
            System.out.println("dataShort fails");
        }

        Text dataText = new Text("hello");
        Text valueText = new Text("intel");
        if (test.evaluate(dataText, valueText).toString().equals("intel")) {
            System.out.println("dataText fails");
        }
    }

    public void testUDFPhone() {
        System.out.println("----------- UDFPhone Test ------------");
        UDFPhone phone = new UDFPhone();
        Text text = new Text("021-123456");

        IntWritable mask = new IntWritable(1);
        Text mText = phone.evaluate(text, mask);
        if (false == mText.equals(new Text("021"))) {
          System.out.printf("testUDFPhone() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText.toString(), mask.get());
        assert (false == mText.equals(new Text("021")));
        }

        IntWritable mask2 = new IntWritable(2);
        Text mText2 = phone.evaluate(text, mask2);
        if (false == mText2.equals(new Text("123456"))) {
          System.out.printf("testUDFPhone() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText2.toString(), mask2.get());
        assert (false == mText2.equals(new Text("123456")));
        }
    }

    public void testUDFMobile() {
        System.out.println("----------- UDFUMobile Test ------------");
        UDFMobile mobile = new UDFMobile();
        Text text = new Text("13834566543");

        IntWritable mask = new IntWritable(2);
        Text mText = mobile.evaluate(text, mask);
        if (false == mText.equals(new Text("138****6543"))) {
          System.out.printf("testUDFMobile() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText.toString(), mask.get());
        }
        assert (true == mText.equals(new Text("138****6543")));

        IntWritable mask2 = new IntWritable(4);
        Text mText2 = mobile.evaluate(text, mask2);
        if (false == mText2.equals(new Text("***34566543"))) {
          System.out.printf("testUDFMobile() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText2.toString(), mask2.get());
        }
        assert (true == mText2.equals(new Text("***34566543")));

        IntWritable mask3 = new IntWritable(5);
        Text mText3 = mobile.evaluate(text, mask3);
        if (false == mText3.equals(new Text("***3456****"))) {
          System.out.printf("testUDFMobile() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText3.toString(), mask3.get());
        }
        assert (true == mText3.equals(new Text("***3456****")));
    }

    public void testUDFEmail() {
        System.out.println("----------- UDFEmail Test ------------");
        UDFEmail email = new UDFEmail();
        Text text = new Text("test@113.com");

        IntWritable mask = new IntWritable(1);
        Text mText = email.evaluate(text, mask);
        if (false == mText.equals(new Text("test"))) {
          System.out.printf("testUDFEmail() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText.toString(), mask.get());
        }
        assert(true == mText.equals(new Text("test")));

        IntWritable mask2 = new IntWritable(2);
        Text mText2 = email.evaluate(text, mask2);
        if (false == mText2.equals(new Text("113.com"))) {
          System.out.printf("testUDFEmail() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText2.toString(), mask2.get());
        }
        assert(true == mText2.equals(new Text("113.com")));
    }

    public void testUDFIP() {
        System.out.println("----------- UDFIP Test ------------");
        UDFIP ip = new UDFIP();
        Text text = new Text("255.255.106.10");

        IntWritable mask = new IntWritable(9);
        Text mText = ip.evaluate(text, mask);
        if (false == mText.equals(new Text("0.255.106.0"))) {
          System.out.printf("testUDFIP() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText.toString(), mask.get());
        }
        assert(true == mText.equals(new Text("0.255.106.0")));

        IntWritable mask2 = new IntWritable(15);
        Text mText2 = ip.evaluate(text, mask2);
        if (false == mText2.equals(new Text("0.0.0.0"))) {
          System.out.printf("testUDFIP() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText2.toString(), mask2.get());
        }
        assert(true == mText2.equals(new Text("0.0.0.0")));
    }

    public void testUDFTimestamp() {
        System.out.println("----------- UDFTimestamp Test ------------");
        UDFTimestamp ts = new UDFTimestamp();
        Timestamp time = Timestamp.valueOf("2016-07-20 21:42:00");
        TimestampWritable text = new TimestampWritable(time);

        IntWritable mask = new IntWritable(7);
        TimestampWritable mText = ts.evaluate(text, mask);
        if (false ==
            mText.equals(new TimestampWritable(Timestamp.valueOf("2016-07-20 00:00:00")))) {
          System.out.printf("testUDFTimestamp() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText.toString(), mask.get());
        }
        assert(true == mText.equals(new TimestampWritable(Timestamp.valueOf("2016-07-20 00:00:00"))));

        IntWritable mask2 = new IntWritable(15);
        TimestampWritable mText2 = ts.evaluate(text, mask2);
        if (false ==
            mText2.equals(new TimestampWritable(Timestamp.valueOf("2016-07-01 00:00:00")))) {
          System.out.printf("testUDFTimestamp() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText2.toString(), mask2.get());
        }
        assert(true == mText2.equals(new TimestampWritable(Timestamp.valueOf("2016-07-01 00:00:00"))));

        IntWritable mask3 = new IntWritable(56);
        TimestampWritable mText3 = ts.evaluate(text, mask3);
        if (false ==
            mText3.equals(new TimestampWritable(Timestamp.valueOf("1960-01-01 21:42:00")))) {
          System.out.printf("testUDFTimestamp() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText3.toString(), mask3.get());
        }
        assert(true == mText3.equals(new TimestampWritable(Timestamp.valueOf("1960-01-01 21:42:00"))));

        IntWritable mask4 = new IntWritable(63);
        TimestampWritable mText4 = ts.evaluate(text, mask4);
        if (false ==
            mText4.equals(new TimestampWritable(Timestamp.valueOf("1960-01-01 00:00:00")))) {
          System.out.printf("testUDFTimestamp() has failed! text=%s, masked_text=%s, mask=%d\n",
              text.toString(), mText4.toString(), mask4.get());
        }
        assert(true == mText4.equals(new TimestampWritable(Timestamp.valueOf("1960-01-01 00:00:00"))));
    }

    public void testUDFHash() {
        System.out.println("----------- UDFHash Test ------------");
        UDFHash hash = new UDFHash();
        Random rand = new Random();
        byte[] str = new byte[100];
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 100; i++) {
            rand.nextBytes(str);
            for (byte b : str) sb.append(b);
            Text test = new Text(sb.toString());
            sb.delete(0, sb.length() - 1);
            hash.evaluate(test);
        }
    }

    public void testUDFUIdentifier() {
        System.out.println("----------- UDFUIdentifier Test ------------");
        UDFUIdentifier uid = new UDFUIdentifier();
        Text text = new Text("rdfuhwqehfihasdihgiauhs");
        Text text2 = new Text("sdfuhwqehfihasdihgiauhs");
        Text result = uid.evaluate(text);

        Text result2 = uid.evaluate(text);
        Text result3 = uid.evaluate(text2);
        Text result4 = uid.evaluate(text, "MD5");

        HashMap<String, Text> items = new HashMap<String, Text>();
        Random rand = new Random();
        byte[] str = new byte[100];
        int item_len = 100000;
        StringBuilder sb = new StringBuilder();
        LocalTime st, et;
        System.out.println("----------- test one ---------------");
        st = LocalTime.now();
        for (int i = 0; i < item_len; i++) {
            //Text test = new Text("rdfuhwqehfihasdihgiauhs" + i);
            rand.nextBytes(str);
            for (byte b : str) sb.append(b);
            Text test = new Text(sb.toString());
            sb.delete(0, sb.length() - 1);
            items.put(uid.evaluate(test).toString(), test);
        }
        et = LocalTime.now();
        System.out.println("item_len = " + item_len + ", map_size = " + items.size());
        System.out.println("[total time]: " + (et.toSecondOfDay() - st.toSecondOfDay()) + "s");

        System.out.println("----------- test two ---------------");
        items.clear();
        st = LocalTime.now();
        for (int i = 0; i < item_len; i++) {
            // Text test = new Text("rdfuhwqehfihasdihgiauhs" + i);
            rand.nextBytes(str);
            for (byte b : str) sb.append(b);
            Text test = new Text(sb.toString());
            sb.delete(0, sb.length() - 1);
            items.put(uid.evaluate(test).toString(), test);
        }
        et = LocalTime.now();
        System.out.println("item_len = " + item_len + ", map_size = " + items.size());
        System.out.println("[total time]: " + (et.toSecondOfDay() - st.toSecondOfDay()) + "s");

        System.out.println("----------- test three ---------------");
        items.clear();
        st = LocalTime.now();
        for (int i = 0; i < item_len; i++) {
            // Text test = new Text("rdfuhwqehfihasdihgiauhs" + i);
            rand.nextBytes(str);
            for (byte b : str) sb.append(b);
            Text test = new Text(sb.toString());
            sb.delete(0, sb.length() - 1);
            items.put(uid.evaluate(test, "MD5").toString(), test);
        }
        et = LocalTime.now();
        System.out.println("item_len = " + item_len + ", map_size = " + items.size());
        System.out.println("[total time]: " + (et.toSecondOfDay() - st.toSecondOfDay()) + "s");
    }

    public void testUDFCipher() {
        System.out.println("----------- UDFCipher Test ------------");
        UDFCipher cipher = new UDFCipher();
        Text text = new Text("This is a test. If you can see the message after decrypted, then it's OK!");
        System.out.println("Before encrypt : " + text);
        Text key = new Text("abcdefghijklmnop");
        // encryption
        Text result = cipher.evaluate(text, Cipher.ENCRYPT_MODE, key);

        // decryption
        Text deresult = cipher.evaluate(result, Cipher.DECRYPT_MODE, key);
        // assert (text == deresult);
    }

}
