package org.shadowmask.core.mask.rules.suppressor;

public interface Suppressor<In, OUT> {
    OUT suppress(In input);
}
