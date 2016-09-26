package org.shadowmask.core.mask.rules;

public class PhoneStrategy extends MaskEngineStrategy {
  public String evaluate(String phone, int mask) {
    if(mask < 0 || mask >3) {
      throw new RuntimeException("mask code for Phone must between 0 and 3!");
    }
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
