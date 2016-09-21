package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.AESCipher;
import org.shadowmask.core.mask.rules.CipherEngine;
import org.shadowmask.core.mask.rules.CipherFactory;

/**
 * UDFCipher with MD5.
 *
 */
@Description(name = "encrytion",
             value = "_FUNC_(x,mode) - returns the encrypted data string of x\n"
                     + "mode - 1 : encryption, 2 : decryption",
             extended = "Example:\n")
public class UDFCipher extends UDF {
    /**
    * mode:
    *   1 encryption
    *   2 decryption
    */
    public Text evaluate(Text content, int mode, Text key) {
      if (content == null) return null;

      CipherEngine ce = CipherFactory.getAESCipher();
      String res = ce.evaluate(content.toString(), mode, key.toString());
      return new Text(res);
    }
}

