package org.shadowmask.core.mask.rules;

public class PhoneStrategy extends MaskEngineStrategy {
  public String evaluate(String phone, int mask) {
    String result = "";
    switch(mask) {
      case 0:
        result = phone;
        break;
      case 1:
        result = phone.substring(0, phone.indexOf('-'));
        break;
      case 2:
        result = phone.substring(phone.indexOf('-') + 1, phone.length());
        break;
      case 3:
        break;
    }
    return result;
  }

}
