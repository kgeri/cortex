package org.ogreg.cortex.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.ogreg.cortex.message.ErrorResponse;
import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.message.MessageCallback;
import org.ogreg.cortex.message.Response;
import org.ogreg.cortex.util.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a duplex connection against a remote host.
 * 
 * @author Gergely Kiss
 */
final class Connection {
	private static final Logger log = LoggerFactory.getLogger(Connection.class);

	private final SocketTransportImpl transport;
	private final Socket socket;

	private final Queue<Message> output = new LinkedBlockingQueue<Message>();
	private final Semaphore semOutput = new Semaphore(0);
	private final Map<Integer, MessageCallback<?>> callbacks = new ConcurrentHashMap<Integer, MessageCallback<?>>();

	private final ChannelThread reader;
	private final ChannelThread writer;

	public Connection(SocketTransportImpl transport, Socket socket) {
		this.transport = transport;
		this.socket = socket;

		SocketAddress address = socket.getRemoteSocketAddress();

		reader = new ChannelReader();
		reader.setName(transport.getBoundPort() + "-ChannelReader-" + address);
		reader.setDaemon(true);

		writer = new ChannelWriter();
		writer.setName(transport.getBoundPort() + "-ChannelWriter-" + address);
		writer.setDaemon(true);
	}

	void start(long until) throws InterruptedException {
		synchronized (writer) {
			writer.start();
			writer.wait(ProcessUtils.check(until, "Timed out before Reader start"));
		}
		synchronized (reader) {
			reader.start();
			reader.wait(ProcessUtils.check(until, "Timed out before Writer start"));
		}
		ProcessUtils.check(until, "Connection start timed out");
	}

	<R> void append(Message message, MessageCallback<R> callback) throws IOException {
		if (callback != null) {
			callbacks.put(message.messageId, callback);
		}
		output.add(message);
		semOutput.release();
	}

	boolean isOpen() {
		// TODO revise this
		return reader.isOpen() && writer.isOpen();
	}

	synchronized void close() {
		reader.close();
		writer.close();
		ProcessUtils.closeQuietly(socket);

		for (MessageCallback<?> callback : callbacks.values()) {
			try {
				callback.onFailure(new IOException("Connection closed"));
			} catch (Exception e) {
				log.error("Unexpected callback failure", e);
			}
		}

		for (Message message : output) {
			synchronized (message) {
				message.notifyAll();
			}
		}
	}

	@Override
	public String toString() {
		return socket.getRemoteSocketAddress() + " [reader=" + reader.state + ", writer="
				+ writer.state + "]";
	}

	enum ConnectionState {
		INIT, OPEN, CLOSING
	}

	abstract class ChannelThread extends Thread {
		ConnectionState state = ConnectionState.INIT;

		void close() {
			state = ConnectionState.CLOSING;
			interrupt();
		}

		boolean isOpen() {
			return state == ConnectionState.OPEN;
		}

		@Override
		public void run() {
			while (true) {
				try {
					establishConnection();

					synchronized (this) {
						state = ConnectionState.OPEN;
						notifyAll();
					}

					processMessages();
				} catch (InterruptedException e) {
					log.debug("{} interrupted", getName());
				} catch (IOException e) {
					log.debug("{} IO error ({})", getName(), e.getLocalizedMessage());
					log.trace("Failure trace", e);
				} catch (Throwable e) {
					log.error(getName() + " FAILED, closing connection", e);
				}

				if (state == ConnectionState.CLOSING) {
					break;
				} else {
					state = ConnectionState.INIT;
					log.debug("Reopening {} in {}ms", getName(), 100);
					try {
						sleep(100);
					} catch (InterruptedException e) {
					}
				}
			}
		}

		protected abstract void establishConnection() throws IOException;

		protected abstract void processMessages() throws InterruptedException, IOException;
	}

	final class ChannelReader extends ChannelThread {
		private ObjectInputStream is;

		@Override
		protected void establishConnection() throws IOException {
			InputStream in = socket.getInputStream();
			is = new ObjectInputStream(in);
		}

		@Override
		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected void processMessages() throws IOException {
			while (!isInterrupted()) {
				// Responding to incoming messages
				Message message;

				try {
					message = (Message) is.readObject();
				} catch (ClassNotFoundException e) {
					log.error(e.getLocalizedMessage());
					// TODO may need to close connection
					continue;
				} catch (ClassCastException e) {
					log.error("Unsupported message type", e);
					continue;
				}

				// If it is a response, then we notify the listeners and remove it
				if (message instanceof Response) {
					Response response = (Response) message;

					MessageCallback callback = callbacks.remove(response.requestId);

					if (callback == null) {
						continue;
					}

					if (response instanceof ErrorResponse) {
						callback.onFailure(((ErrorResponse) response).getError());
					} else {
						try {
							callback.onSuccess(response.value);
						} catch (ClassCastException e) {
							log.error("Unexpected response type", e);
						} catch (Exception e) {
							log.error("Unexpected callback failure", e);
						}
					}
				}
				// Otherwise we try to serve the request
				else {
					Message response = transport.execute(message);
					append(response, null);
				}
			}
		}
	}

	final class ChannelWriter extends ChannelThread {
		private ObjectOutputStream os;

		@Override
		protected void establishConnection() throws IOException {
			OutputStream out = socket.getOutputStream();
			os = new ObjectOutputStream(out);
			os.flush();
		}

		@Override
		protected void processMessages() throws InterruptedException, IOException {
			while (!isInterrupted()) {
				semOutput.acquire();
				Message message = output.poll();
				if (message != null) {
					os.writeObject(message);
					os.reset();
					os.flush();
				}
			}
		}
	}
}