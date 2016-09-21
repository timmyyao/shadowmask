package org.shadowmask.core.mask.rules;

public class UDAFObject {
  protected String row_key;
  protected int count = 0;

  public UDAFObject(String code) {
    count = 1;
    row_key = code;
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
    UDAFObject row = (UDAFObject) obj;
    if(!row_key.equals(row.row_key)) return false;
    return true;
  }

  public String getRow() {
    return row_key;
  }

  public Integer getCount() {
    return count;
  }

  public void setCount(Integer count) {
    this.count = count;
  }

  public void increase(Integer cnt) {
    this.count += cnt;
  }
}
