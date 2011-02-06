package org.ogreg.cortex.transport;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;

import org.ogreg.cortex.message.ErrorResponse;
import org.ogreg.cortex.message.Message;

/**
 * SPI interface for socket-based {@link Transport}s.
 * 
 * @author Gergely Kiss
 */
interface SocketTransport extends Transport {

	/**
	 * Opens a socket on the specified <code>address</code>.
	 * 
	 * @param address The address to open the new socket to
	 * @param until The deadline to wait until the socket is opened
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	Socket openSocket(SocketAddress address, long until) throws IOException, InterruptedException;

	/**
	 * Executes <code>message</code>, and sends a response.
	 * <p>
	 * Should never throw an error, since it's called by the reader thread. On failures, this method
	 * should return {@link ErrorResponse}s.
	 * </p>
	 * 
	 * @param message
	 * @return
	 */
	void execute(SocketAddress source, Message message);
}
