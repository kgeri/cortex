package org.ogreg.cortex.transport;

import java.net.Socket;

import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.message.MessageCallback;

/**
 * SPI interface for classes which hold the state of a client connection.
 * 
 * @author Gergely Kiss
 */
interface ClientChannel {

	/**
	 * Processes the incoming message.
	 * 
	 * @param message
	 */
	void process(Message message);

	/**
	 * Offers <code>message</code> to this channel's output queue, and notifies
	 * <code>callback</code> when the response is received.
	 * <p>
	 * Note: if invoked with a null callback, this method does not overwrite the previously set
	 * callback.
	 * </p>
	 * 
	 * @param <R> The expected type of the response
	 * @param message The message to send
	 * @param callback The callback to notify when the response is received, or null if no
	 *            notification is required
	 */
	<R> void offer(Message message, MessageCallback<R> object);

	/**
	 * Ensures that this channel has an open connection.
	 * 
	 * @param socket The socket to use for this channel, or null if the channel should use its
	 *            current socket
	 * @param until The time to wait until the connection is open
	 * @return
	 * @throws InterruptedException if the connection is still not open at <code>until</code>
	 */
	ClientChannel ensureOpen(Socket socket, long until) throws InterruptedException;

	/**
	 * Disposes of the channel's state, and closes all connections quietly.
	 */
	void destroy();

	Message takeOutput() throws InterruptedException;
}
