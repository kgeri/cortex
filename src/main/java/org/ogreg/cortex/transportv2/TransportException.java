package org.ogreg.cortex.transportv2;

/**
 * Signals a transport state or configuration error.
 * 
 * @author Gergely Kiss
 */
public class TransportException extends RuntimeException {
	private static final long serialVersionUID = 7220746801726642321L;

	public TransportException(String message) {
		super(message);
	}
	
	public TransportException(String message, Throwable cause) {
		super(message, cause);
	}
}
