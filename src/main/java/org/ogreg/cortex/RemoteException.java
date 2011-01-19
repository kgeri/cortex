package org.ogreg.cortex;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Signals an error which originated from a remote JVM.
 * 
 * @author Gergely Kiss
 */
public class RemoteException extends Exception {
	private static final long serialVersionUID = 1945501393907305617L;
	private static String hostName;

	static {
		try {
			InetAddress address = InetAddress.getLocalHost();
			hostName = address.getCanonicalHostName() + " (" + address.getHostAddress() + ")";
		} catch (UnknownHostException e) {
			hostName = "<unknown host>";
		}
	}

	public RemoteException(Throwable cause) {
		super(hostName, cause);
	}

	public RemoteException(String message, Throwable cause) {
		super(hostName + ": " + message, cause);
	}
}
