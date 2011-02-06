package org.ogreg.cortex.transport;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.ogreg.cortex.message.Message;

/**
 * Connection thread for sending requests to a remote host.
 * 
 * @author Gergely Kiss
 */
final class ChannelWriter extends AbstractChannelProcess {
	private ObjectOutputStream os;

	public ChannelWriter(ClientChannel channel, Socket socket) {
		super(channel, socket);
	}

	@Override
	protected void establishConnection(Socket socket) throws IOException {
		OutputStream out = socket.getOutputStream();
		os = new ObjectOutputStream(out);
		os.flush();
	}

	@Override
	protected void processMessage() throws InterruptedException, IOException {
		ClientChannel channel = ensureAttached();
		Message message = channel.takeOutput();
		if (message != null) {
			try {
				os.writeObject(message);
				os.reset();
				os.flush();
				System.out.println(getName() + " SENT: " + message);
			} catch (IOException e) {
				channel.offer(message, null);
				throw e;
			}
		}
	}
}