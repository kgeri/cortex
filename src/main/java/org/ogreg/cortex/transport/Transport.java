package org.ogreg.cortex.transport;

import java.io.Closeable;
import java.net.SocketAddress;

import org.ogreg.cortex.RemoteException;
import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.message.MessageCallback;

/**
 * Common interface for components providing remote function call capabilities.
 * 
 * @author Gergely Kiss
 */
public interface Transport extends Closeable {

	/**
	 * Sends <code>message</code> to <code>address</code> using the specified <code>timeOut</code>
	 * and returns the response synchronously or throws an error.
	 * 
	 * @param address The address of the target host to send the message to
	 * @param message The message to send (like an {@link org.ogreg.cortex.message.Invocation})
	 * @param timeOut The maximum time allowed in milliseconds
	 * @return
	 * @throws NullPointerException if address or message is null
	 * @throws InterruptedException on timeout
	 * @throws RemoteException if the remote service has thrown an exception
	 */
	Object callSync(SocketAddress address, Message message, long timeOut)
			throws InterruptedException, RemoteException;

	/**
	 * Sends <code>message</code> to <code>address</code> asynchronously, and notifies
	 * <code>callback</code> when the result has arrived.
	 * 
	 * @param address The address of the target host to send the message to
	 * @param message The message to send (like an {@link org.ogreg.cortex.message.Invocation})
	 * @param timeOut The maximum time allowed (for connection establishment!) in milliseconds
	 * @param callback The callback to notify when the response is received
	 * @return
	 * @throws NullPointerException if address or message is null
	 * @throws InterruptedException on connection timeout
	 */
	<R> void callAsync(SocketAddress address, Message message, long timeOut,
			MessageCallback<R> callback) throws InterruptedException;

	/**
	 * Sends <code>message</code> to <code>address</code> asynchronously and does not bother with
	 * response handling at all.
	 * 
	 * @param address The address of the target host to send the message to
	 * @param message The message to send (like an {@link org.ogreg.cortex.message.Invocation})
	 * @param timeOut The maximum time allowed (for connection establishment!) in milliseconds
	 * @param callback The callback to notify when the response is received
	 * @return
	 * @throws NullPointerException if address or message is null
	 * @throws InterruptedException on connection timeout
	 */
	void callAsync(SocketAddress address, Message message, long timeOut)
			throws InterruptedException;

	/**
	 * Opens the transport channel, preparing it for passing messages.
	 * 
	 * @param timeOut The maximum number of milliseconds to wait for the server listener to open
	 * @throws InterruptedException if the transport open timed out
	 */
	void open(long timeOut) throws InterruptedException;
}
