package org.shadowmask.core.mask.rules;

public class MaskEngine {
    private MaskEngineStrategy maskEngine = null;

    public MaskEngine(MaskEngineStrategy mes) {
      this.maskEngine = mes;
    }

    public String evaluate(String value, int mask) {
        return this.maskEngine.evaluate(value, mask);
    }
}
