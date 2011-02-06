package org.ogreg.cortex.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.Socket;

import org.ogreg.cortex.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection thread for reading remote requests.
 * 
 * @author Gergely Kiss
 */
final class ChannelReader extends AbstractChannelProcess {
	private static final Logger log = LoggerFactory.getLogger(ChannelReader.class);

	private ObjectInputStream is;

	public ChannelReader(ClientChannel channel, Socket socket) {
		super(channel, socket);
	}

	@Override
	protected void establishConnection(Socket socket) throws IOException {
		InputStream in = socket.getInputStream();
		is = new ObjectInputStream(in);
	}

	@Override
	protected void processMessage() throws IOException {
		ClientChannel channel = ensureAttached();
		Message message;

		try {
			message = (Message) is.readObject();
			System.out.println(getName() + " RECV: " + message);
		} catch (ClassNotFoundException e) {
			log.error(e.getLocalizedMessage());
			throw new IOException(e);
		} catch (ClassCastException e) {
			log.error("Unsupported message type", e);
			return;
		}

		channel.processInput(message);
	}
}