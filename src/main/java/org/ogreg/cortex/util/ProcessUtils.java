package org.ogreg.cortex.util;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Common helper methods for multi-threaded processing.
 * 
 * @author Gergely Kiss
 */
public class ProcessUtils {

	/**
	 * Ensures that the current execution is within time limits.
	 * 
	 * @param time The point in time until the current execution should finish
	 * @param message The message to show if the current execution is timed out
	 * @return The remaining time until <code>time</code>
	 * @throws InterruptedException if <code>( {@link Thread#isInterrupted()} ||
	 *             {@link System#currentTimeMillis()} &gt; time - 1 )</code>
	 */
	public static long check(long time, String message) throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		} else {
			long remaining = time - System.currentTimeMillis();
			if (remaining < 1) {
				throw new InterruptedException(message);
			} else {
				return remaining;
			}
		}
	}

	/**
	 * Closes the <code>closeable</code> ignoring any IO errors.
	 * 
	 * @param closeable
	 */
	public static final void closeQuietly(Closeable closeable) {
		try {
			closeable.close();
		} catch (IOException e) {
		}
	}
	
	/**
	 * Closes the <code>socket</code> ignoring any IO errors.
	 * 
	 * @param closeable
	 */
	public static final void closeQuietly(Socket socket) {
		try {
			socket.close();
		} catch (IOException e) {
		}
	}
	
	/**
	 * Closes the <code>socket</code> ignoring any IO errors.
	 * 
	 * @param closeable
	 */
	public static final void closeQuietly(ServerSocket socket) {
		try {
			socket.close();
		} catch (IOException e) {
		}
	}
}
