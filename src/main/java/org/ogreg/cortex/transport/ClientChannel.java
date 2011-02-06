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
	<R> void send(Message message, MessageCallback<R> object);

	/**
	 * Ensures that this channel has an open connection for reading and writing.
	 * 
	 * @param socket The socket to use for this channel, or null if the channel should open a new
	 *            socket
	 * @param until The deadline to wait until the connection is open
	 * @return this channel
	 * @throws InterruptedException if the connection is still not open at <code>until</code>
	 */
	ClientChannel ensureOpen(Socket socket, long until) throws InterruptedException;

	/**
	 * Disposes of the channel's state, and closes all connections quietly.
	 */
	void destroy();

	/**
	 * Processes the incoming message. Used by the {@link ChannelReader}.
	 * 
	 * @param message
	 */
	void processInput(Message message);

	/**
	 * Reads the next output message from this channel's output queue. The call blocks until one is
	 * available. Used by the {@link ChannelWriter}.
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	Message takeOutput() throws InterruptedException;
}
