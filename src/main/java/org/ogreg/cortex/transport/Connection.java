package org.ogreg.cortex.transport;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;

import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.util.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a duplex connection to a remote host.
 * 
 * @author Gergely Kiss
 */
final class Connection implements Closeable {
	private static final Logger log = LoggerFactory.getLogger(Connection.class);

	private final Socket socket;
	private final ChannelThread reader;
	private final ChannelThread writer;
	private final ClientChannel channel;

	public Connection(Socket socket, ClientChannel channel) {
		this.socket = socket;
		this.channel = channel;

		SocketAddress address = socket.getRemoteSocketAddress();

		reader = new ChannelReader();
		reader.setName("ChannelReader-" + address);
		reader.setDaemon(true);

		writer = new ChannelWriter();
		writer.setName("ChannelWriter-" + address);
		writer.setDaemon(true);
	}

	@Override
	public synchronized void close() throws IOException {
		reader.close();
		writer.close();
		socket.close();
	}

	public synchronized void start(long until) throws InterruptedException {
		synchronized (writer) {
			if (!writer.isAlive()) {
				writer.start();
			}
			writer.wait(ProcessUtils.check(until, "Timed out before Reader start"));
		}
		synchronized (reader) {
			if (!reader.isAlive()) {
				reader.start();
			}
			reader.wait(ProcessUtils.check(until, "Timed out before Writer start"));
		}
		ProcessUtils.check(until, "Connection start timed out");
	}

	boolean isOpen() {
		return reader.open && writer.open;
	}

	Socket getSocket() {
		return socket;
	}

	@Override
	public String toString() {
		return socket.getRemoteSocketAddress() + " [readerOpen=" + reader.open + ", writerOpen="
				+ writer.open + "]";
	}

	/**
	 * Common base class for connection R/W threads.
	 * 
	 * @author Gergely Kiss
	 */
	private abstract class ChannelThread extends Thread {
		private boolean open = false;

		private void close() {
			open = false;
			interrupt();
		}

		@Override
		public void run() {
			try {
				establishConnection(socket);

				synchronized (this) {
					open = true;
					notifyAll();
				}

				while (!isInterrupted() && open) {
					processMessage();
				}
			} catch (InterruptedException e) {
				log.debug("{} interrupted", getName());
			} catch (IOException e) {
				log.debug("{} IO error ({})", getName(), e.getLocalizedMessage());
				log.debug("Failure trace", e);
			} catch (Throwable e) {
				log.error(getName() + " FAILED, closing connection", e);
			} finally {
				open = false;
			}
		}

		/**
		 * Implementors should initialize the connection here.
		 * 
		 * @param socket
		 * @throws IOException if initialization failed
		 */
		protected abstract void establishConnection(Socket socket) throws IOException;

		/**
		 * Implementors should process one incoming/outgoing message here.
		 * 
		 * @throws IOException if processing failed
		 * @throws InterruptedException if execution was interrupted
		 */
		protected abstract void processMessage() throws IOException, InterruptedException;
	}

	/**
	 * Connection thread for reading remote requests.
	 * 
	 * @author Gergely Kiss
	 */
	private final class ChannelReader extends ChannelThread {
		private ObjectInputStream is;

		@Override
		protected void establishConnection(Socket socket) throws IOException {
			InputStream in = socket.getInputStream();
			is = new ObjectInputStream(in);
		}

		@Override
		protected void processMessage() throws IOException {
			// Responding to incoming messages
			Message message;

			try {
				message = (Message) is.readObject();
			} catch (ClassNotFoundException e) {
				log.error(e.getLocalizedMessage());
				// TODO may need to close connection
				return;
			} catch (ClassCastException e) {
				log.error("Unsupported message type", e);
				return;
			}

			channel.process(message);
		}
	}

	/**
	 * Connection thread for sending requests to a remote host.
	 * 
	 * @author Gergely Kiss
	 */
	private final class ChannelWriter extends ChannelThread {
		private ObjectOutputStream os;

		@Override
		protected void establishConnection(Socket socket) throws IOException {
			OutputStream out = socket.getOutputStream();
			os = new ObjectOutputStream(out);
			os.flush();
		}

		@Override
		protected void processMessage() throws InterruptedException, IOException {
			Message message = channel.waitForOutput();
			if (message != null) {
				try {
					os.writeObject(message);
					os.reset();
					os.flush();
				} catch (IOException e) {
					channel.offer(message, null);
					throw e;
				}
			}
		}
	}
}