package org.ogreg.cortex.transport;

import java.io.IOException;
import java.io.ObjectOutputStream;
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

class ClientChannelImpl implements ClientChannel {
	private static final Logger log = LoggerFactory.getLogger(ClientChannelImpl.class);

	private final SocketTransportImpl socketTransportImpl;
	private final SocketAddress address;

	private final Queue<Message> output = new LinkedBlockingQueue<Message>();
	private final Semaphore semOutput = new Semaphore(0);
	private final Map<Integer, MessageCallback<?>> callbacks = new ConcurrentHashMap<Integer, MessageCallback<?>>();

	AbstractChannelProcess reader;
	AbstractChannelProcess writer;

	public ClientChannelImpl(SocketTransportImpl socketTransportImpl, SocketAddress address) {
		this.socketTransportImpl = socketTransportImpl;
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
					socket = openSocket(address, socket, until);
					writer = new ChannelWriter(this, socket);
					writer.setName("ChannelWriter-" + address);
					writer.open(until);
				} else if (!writer.isOpen() || (socket != null && writer.getSocket() != socket)) {
					log.debug("Reopening writer to: {}", address);
					ProcessUtils.closeQuietly(writer);
					socket = openSocket(address, socket, until);
					writer = new ChannelWriter(this, socket);
					writer.setName("ChannelWriter-" + address);
					writer.open(until);
				}

				// Ensure reader is open and bound to the socket if specified
				if (reader == null) {
					log.debug("Opening reader to: {}", address);
					socket = openSocket(address, socket, until);
					reader = new ChannelReader(this, socket);
					reader.setName("ChannelReader-" + address);
					reader.open(until);
				} else if (!reader.isOpen() || (socket != null && reader.getSocket() != socket)) {
					log.debug("Reopening reader to: {}", address);
					ProcessUtils.closeQuietly(reader);
					socket = openSocket(address, socket, until);
					reader = new ChannelReader(this, socket);
					reader.setName("ChannelReader-" + address);
					reader.open(until);
				}

				return this;
			} catch (IOException e) {
				// TODO separate causes
				log.error("Failed to open connection to '{}' ({}), retrying in 100ms", address,
						e.getLocalizedMessage());
				Thread.sleep(100);
				ProcessUtils.check(until, "Connection open timed out");
			}
		}
	}

	private Socket openSocket(SocketAddress address, Socket socket, long until) throws IOException,
			InterruptedException {
		if (socket == null) {
			socket = new Socket();
			socket.setKeepAlive(true);
			socket.setReuseAddress(true);
			socket.setReceiveBufferSize(this.socketTransportImpl.socketBufferSize);
			socket.setSendBufferSize(this.socketTransportImpl.socketBufferSize);
			socket.setSoTimeout(5000); // TODO timeout
			socket.connect(address,
					(int) ProcessUtils.check(until, "Timed out before socket connect"));
			log.debug("Opened socket to: {}", address);

			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			oos.writeObject(this.socketTransportImpl.getBoundAddress());
		} else {
			log.debug("Reused existing socket to: {}", address);
		}
		return socket;
	}

	@Override
	public <R> void offer(Message message, MessageCallback<R> callback) {
		if (callback != null) {
			callbacks.put(message.messageId, callback);
		}
		output.add(message);
		semOutput.release();
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void process(Message message) {
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
			this.socketTransportImpl.execute(address, message);
		}
	}

	@Override
	public Message waitForOutput() throws InterruptedException {
		semOutput.acquire();
		return output.poll();
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

		for (Message message : output) {
			synchronized (message) {
				message.notifyAll();
			}
		}

		if (reader != null) {
			ProcessUtils.closeQuietly(reader);
		}
		if (writer != null) {
			ProcessUtils.closeQuietly(writer);
		}
	}
}