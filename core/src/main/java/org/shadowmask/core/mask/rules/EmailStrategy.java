package org.shadowmask.core.mask.rules;

public class EmailStrategy extends MaskEngineStrategy {
  @Override
  public String evaluate(String value, int mask) {
    if(mask < 0 || mask >3) {
      throw new RuntimeException("mask code for Email must between 0 and 3!");
    }
    String result = "";
    switch(mask) {
      case 0:
        result = value;
          break;
      case 1:
        result = value.substring(0, value.indexOf('@'));
        break;
      case 2:
        result = value.substring(value.indexOf('@') + 1, value.length());
        break;
      case 3:
        break;
    }
    return result;
  }

}
