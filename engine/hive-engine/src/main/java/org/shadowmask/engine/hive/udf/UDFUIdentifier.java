package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.UIDGenerator;
import org.shadowmask.core.mask.rules.ShadowUID;

/**
 * UDFUIdentifier.
 *
 */
@Description(name = "unique indentifier",
             value = "_FUNC_(x) - returns a unique identifier of x",
             extended = "Example:\n")
public class UDFUIdentifier extends UDF {
  /* Method-1: use UUID, the best efficiency*/
  public Text evaluate(Text content) {
    if (content == null) return null;
    String str = content.toString();

    ShadowUID uid = UIDGenerator.getDefaultUID();
    str = uid.evaluate(str);

    return new Text(str);
  }

  /* Method-2: use MD5/SHA/..*/
  public Text evaluate(Text content, String encryptor) {
    if (content == null) return null;
    String str = content.toString();

    ShadowUID uid = UIDGenerator.getSignitureUID("MD5");
    String res = uid.evaluate(str);

    if (res != null) return new Text(res);
    return null;
  }
}
