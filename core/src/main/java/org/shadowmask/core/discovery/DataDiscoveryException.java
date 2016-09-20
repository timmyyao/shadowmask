package org.shadowmask.core.discovery;

public class DataDiscoveryException extends RuntimeException {

    public DataDiscoveryException() {
    }

    public DataDiscoveryException(Throwable e) {
        super(e);
    }

    public DataDiscoveryException(String message) {
        super(message);
    }

    public DataDiscoveryException(String message, Throwable e) {
        super(message, e);
    }
}
