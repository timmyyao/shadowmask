package org.shadowmask.core.mask.rules;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class UDAFLDObject extends UDAFObject {
  private String sensitive_value;
  private Map<String, Integer> deversities;
  public UDAFLDObject(String code, String value) {
    super(code);
    this.sensitive_value = value;
    this.deversities = new HashMap<String, Integer>();
    this.deversities.put(sensitive_value, 1);
  }

  @Override
  public int hashCode() {
    int hash = 0;
    hash = row_key.hashCode();
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
    UDAFLDObject row = (UDAFLDObject) obj;
    if(!row_key.equals(row.row_key)) return false;
    return true;
  }

  @Override
  public String getRow() {
    return row_key;
  }

  @Override
  public Integer getCount() {
    return deversities.size();
  }

  public String getSensitiveValue() {
    return sensitive_value;
  }

  public HashMap<String, Integer> getDeversities() {
    return (HashMap<String, Integer>) deversities;
  }

}
