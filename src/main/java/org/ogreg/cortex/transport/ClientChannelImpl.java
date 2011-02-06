package org.ogreg.cortex.transport;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.ogreg.cortex.message.ErrorResponse;
import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.message.MessageCallback;
import org.ogreg.cortex.message.Response;
import org.ogreg.cortex.util.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default client channel implementation.
 * <p>
 * The {@link ClientChannel} is responsible for holding the state of the connection to a specified
 * remote address. The channel is asynchronous, messages may be sent by
 * {@link #send(Message, MessageCallback)}-ing them through the channel's output queue, and
 * responses may be received by registering a callback.
 * </p>
 * 
 * @author Gergely Kiss
 */
class ClientChannelImpl implements ClientChannel {
	private static final Logger log = LoggerFactory.getLogger(ClientChannelImpl.class);

	private final SocketTransport parent;
	private final SocketAddress address;

	private final BlockingQueue<Message> output = new LinkedBlockingQueue<Message>();
	private final Map<Integer, MessageCallback<?>> callbacks = new ConcurrentHashMap<Integer, MessageCallback<?>>();

	AbstractChannelProcess reader;
	AbstractChannelProcess writer;

	public ClientChannelImpl(SocketTransport parent, SocketAddress address) {
		this.parent = parent;
		this.address = address;
	}

	@Override
	public synchronized ClientChannel ensureOpen(Socket socket, long until)
			throws InterruptedException {
		while (true) {
			try {
				// Ensure writer is open and bound to the socket if specified
				if (writer == null) {
					log.debug("Opening writer to: {}", address);
					socket = openSocket(socket, until);
					writer = new ChannelWriter(this, socket);
					writer.setName("ChannelWriter-" + address);
					writer.open(until);
				} else if (!writer.isOpen() || (socket != null && writer.getSocket() != socket)) {
					log.debug("Reopening writer to: {}", address);
					ProcessUtils.closeQuietly(writer);
					socket = openSocket(socket, until);
					writer = new ChannelWriter(this, socket);
					writer.setName("ChannelWriter-" + address);
					writer.open(until);
				}

				// Ensure reader is open and bound to the socket if specified
				if (reader == null) {
					log.debug("Opening reader to: {}", address);
					socket = openSocket(socket, until);
					reader = new ChannelReader(this, socket);
					reader.setName("ChannelReader-" + address);
					reader.open(until);
				} else if (!reader.isOpen() || (socket != null && reader.getSocket() != socket)) {
					log.debug("Reopening reader to: {}", address);
					ProcessUtils.closeQuietly(reader);
					socket = openSocket(socket, until);
					reader = new ChannelReader(this, socket);
					reader.setName("ChannelReader-" + address);
					reader.open(until);
				}

				return this;
			} catch (IOException e) {
				// TODO separate causes
				log.error("Failed to open connection to '{}' ({}), retrying in 100ms", address,
						e.getLocalizedMessage());

				// TODO magic number
				Thread.sleep(100);
				ProcessUtils.check(until, "Connection open timed out");
			}
		}
	}

	private Socket openSocket(Socket socket, long until) throws IOException, InterruptedException {
		if (socket == null) {
			socket = parent.openSocket(address, until);
			log.debug("Opened socket to: {}", address);
		} else {
			log.debug("Reused existing socket to: {}", address);
		}
		return socket;
	}

	@Override
	public <R> void send(Message message, MessageCallback<R> callback) {
		if (callback != null) {
			callbacks.put(message.messageId, callback);
		}
		output.offer(message);
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void processInput(Message message) {
		// If it is a response, then we notify the listeners and remove it
		if (message instanceof Response) {
			Response response = (Response) message;

			MessageCallback callback = callbacks.remove(response.requestId);

			if (callback == null) {
				return;
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
			parent.execute(address, message);
		}
	}

	@Override
	public void destroy() {
		for (MessageCallback<?> callback : callbacks.values()) {
			try {
				callback.onFailure(new IOException("Connection closed"));
			} catch (Exception e) {
				log.error("Unexpected callback failure", e);
			}
		}
		callbacks.clear();

		for (Message message : output) {
			synchronized (message) {
				message.notifyAll();
			}
		}
		output.clear();

		if (reader != null) {
			ProcessUtils.closeQuietly(reader);
		}
		if (writer != null) {
			ProcessUtils.closeQuietly(writer);
		}
	}

	@Override
	public Message takeOutput() throws InterruptedException {
		return output.take();
	}
}