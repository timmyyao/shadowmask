package org.shadowmask.engine.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.shadowmask.core.mask.rules.MaskEngine;
import org.shadowmask.core.mask.rules.EmailStrategy;

/**
 * UDFEmail.
 *
 */
@Description(name = "email",
             value = "_FUNC_(email, mask) - returns the masked value of email\n"
                + "email - original email string combined with an account name and an email domain like xx@xx\n"
                + "mask - hide the user name or the email domain: 1 masked, 0 unmasked",
             extended = "Example:\n")
public class UDFEmail extends UDF {
  /* mask XXX@YYY:
   * 0-(00)2  XXX@YYY
   * 1-(01)2  XXX
   * 2-(10)2  YYY
   * 3-(11)2  
   */
  public Text evaluate(Text email, IntWritable mask) {
    if (email == null || mask == null) return null;
    int mode = mask.get();
    String str_email = email.toString();
    Text result = new Text();

    MaskEngine me = new MaskEngine(new EmailStrategy());

    result.set(me.evaluate(str_email, mode));
    return result;
  }
}
