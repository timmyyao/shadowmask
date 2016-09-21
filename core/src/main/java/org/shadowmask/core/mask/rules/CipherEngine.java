package org.shadowmask.core.mask.rules;

abstract public class CipherEngine {
    protected String method;
    abstract public String evaluate(String content, int mode, String key);
    abstract protected String encrypt(byte[] content);
    abstract protected String decrypt(byte[] content);
}
