package org.shadowmask.core.mask.rules;

import org.shadowmask.core.mask.rules.AESCipher;

public class CipherFactory {
  public static CipherEngine getAESCipher() {
    return new AESCipher();
  }
}
