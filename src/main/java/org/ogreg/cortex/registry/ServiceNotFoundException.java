package org.ogreg.cortex.registry;

/**
 * Signals an error which originated from a remote JVM.
 * 
 * @author Gergely Kiss
 */
public class ServiceNotFoundException extends Exception {
	private static final long serialVersionUID = 1945501393907305617L;

	public ServiceNotFoundException(String message) {
		super(message);
	}

	public ServiceNotFoundException(Throwable cause) {
		super(cause);
	}

	public ServiceNotFoundException(String message, Throwable cause) {
		super(message, cause);
	}
}
