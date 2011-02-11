package org.ogreg.cortex.transportv2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.ogreg.cortex.RemoteException;
import org.ogreg.cortex.message.ErrorResponse;
import org.ogreg.cortex.message.Invocation;
import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.message.MessageCallback;
import org.ogreg.cortex.message.Response;
import org.ogreg.cortex.registry.ServiceRegistry;
import org.ogreg.cortex.util.ByteBufferInputStream;
import org.ogreg.cortex.util.ByteBufferOutputStream;
import org.ogreg.cortex.util.DatagramChannelBuilder;
import org.ogreg.cortex.util.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection manager for opening, closing and pooling standard (blocking) socket connections
 * towards other members of the cortex, and binding this node's server socket.
 * 
 * @author Gergely Kiss
 */
public class DatagramTransportImpl implements Transport {
	static final Logger log = LoggerFactory.getLogger(DatagramTransportImpl.class);

	/** The start of the usable port range (inclusive). */
	private int minPort = 4000;

	/** The end of the usable port range (inclusive). */
	private int maxPort = 4100;

	/** The size of the socket buffers. Default: 65535. */
	private int socketBufferSize = 0x10000;

	/**
	 * The SO_TIMEOUT value for all opened sockets. Default: 5000 ms.
	 */
	int soTimeOut = 5000;

	/** The service registry used to service requests. */
	private ServiceRegistry registry;

	private final BlockingQueue<TransportMessage> output = new LinkedBlockingQueue<TransportMessage>();
	private final Map<Integer, MessageCallback<?>> callbacks = new ConcurrentHashMap<Integer, MessageCallback<?>>();

	/** Parallel executor service for processing incoming requests. */
	ExecutorService executor;

	volatile RequestListener listener;
	ResponseWriter writer;

	private DatagramChannelBuilder channelBuilder;
	private SelectionKey channelKey;
	private Selector selector;
	DatagramChannel channel;

	/**
	 * Initializes the connector. Must be called prior to {@link #open()}.
	 */
	@PostConstruct
	public void init() {
		// TODO Properties
		int corePoolSize = 1;
		int maxPoolSize = 10;

		executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, 1, TimeUnit.MINUTES,
				new LinkedBlockingQueue<Runnable>());
	}

	@Override
	public void open(long timeOut) throws InterruptedException, IOException {
		if (listener != null && listener.isAlive()) {
			return;
		}
		listener = new RequestListener();

		synchronized (listener) {
			long until = System.currentTimeMillis() + timeOut;
			channelBuilder = new DatagramChannelBuilder().minPort(minPort).maxPort(maxPort)
					.bufferSize(socketBufferSize).soTimeOut(soTimeOut).log(log);

			if (selector != null) {
				selector.close();
			}
			selector = Selector.open();

			try {
				listener.start();
				listener.wait(ProcessUtils.check(until, "RequestListener open timed out"));
				ProcessUtils.check(until, "RequestListener open timed out");
			} catch (InterruptedException e) {
				ProcessUtils.closeQuietly(this);
				throw e;
			}

			writer = new ResponseWriter();
			writer.start();
		}
	}

	private DatagramChannel ensureChannel() throws BindException, IOException {
		if (channel != null) {
			if (channel.isOpen()) {
				return channel;
			}
			System.err.println("REOPENED");
		} else {
			System.err.println("OPENED");
		}

		synchronized (this) {
			closeChannel();

			channel = channelBuilder.bind();
			channelKey = channel.register(selector, SelectionKey.OP_READ);

			return channel;
		}
	}

	private synchronized void closeChannel() {
		if (channelKey != null) {
			channelKey.cancel();
		}
		ProcessUtils.closeQuietly(channel);
	}

	/**
	 * Stops listening and closes all server and client connections.
	 */
	@Override
	public synchronized void close() throws IOException {
		closeChannel();

		if (listener != null) {
			listener.interrupt();
		}
		if (writer != null) {
			writer.interrupt();
		}

		for (MessageCallback<?> callback : callbacks.values()) {
			try {
				callback.onFailure(new IOException("Connection closed"));
			} catch (Exception e) {
				log.error("Unexpected callback failure", e);
			}
		}
		callbacks.clear();

		for (TransportMessage message : output) {
			synchronized (message) {
				message.message.notifyAll();
			}
		}
		output.clear();

		listener = null;
		log.info("Socket transport closed");
	}

	@Override
	public Object callSync(SocketAddress address, Message message, long timeOut)
			throws InterruptedException, RemoteException {
		long until = System.currentTimeMillis() + timeOut;
		SyncMessageCallback<Object> callback = new SyncMessageCallback<Object>(message);
		send(address, message, callback);
		return callback.waitUntil(until);
	}

	@Override
	public <R> void callAsync(SocketAddress address, Message message, MessageCallback<R> callback)
			throws InterruptedException {
		send(address, message, callback);
	}

	@Override
	public void callAsync(SocketAddress address, Message message) throws InterruptedException {
		send(address, message, null);
	}

	private <R> void send(SocketAddress address, Message message, MessageCallback<R> callback) {
		if (callback != null) {
			callbacks.put(message.messageId, callback);
		}
		output.offer(new TransportMessage(address, message));
	}

	protected void finalize() throws Throwable {
		close();
	}

	private void processRequest(final SocketAddress source, final Message message) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				Response rsp = execute(message);

				try {
					callAsync(source, rsp);
				} catch (Exception e) {
					log.error("Failed to send response", e);
				}
			}
		});
	}

	private Response execute(Message message) {
		if (message instanceof Invocation) {
			Invocation m = (Invocation) message;

			try {
				Object service = registry.getService(m.getType(), m.getIdentifier());
				return new Response(message.messageId, m.invoke(service));
			} catch (InvocationTargetException e) {
				return new ErrorResponse(message.messageId, e.getCause());
			} catch (Exception e) {
				return new ErrorResponse(message.messageId, e);
			}
		} else {
			return new ErrorResponse(message.messageId, new UnsupportedOperationException(
					"Unsupported message: " + message));
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void process(SocketAddress address, Message message) throws IOException {
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
			processRequest(address, message);
		}
	}

	/**
	 * Returns the address on which the connector is listening, or null if it is not.
	 * 
	 * @return
	 */
	public InetSocketAddress getBoundAddress() {
		return (InetSocketAddress) (channel == null ? null : channel.socket()
				.getLocalSocketAddress());
	}

	public void setRegistry(ServiceRegistry registry) {
		this.registry = registry;
	}

	public void setMinPort(int minPort) {
		this.minPort = minPort;
	}

	public void setMaxPort(int maxPort) {
		this.maxPort = maxPort;
	}

	public void setSocketBufferSize(int socketBufferSize) {
		this.socketBufferSize = socketBufferSize;
	}

	public void setSoTimeOut(int soTimeOut) {
		this.soTimeOut = soTimeOut;
	}

	private final class TransportMessage {
		final SocketAddress address;
		final Message message;

		public TransportMessage(SocketAddress address, Message message) {
			this.address = address;
			this.message = message;
		}
	}

	// Server listener thread
	final class RequestListener extends Thread {
		private ByteBuffer inputBuffer;

		public RequestListener() {
			this.inputBuffer = ByteBuffer.allocateDirect(socketBufferSize);

			setName("RequestListener");
			setDaemon(true);
		}

		@Override
		public void run() {
			try {

				while (!isInterrupted()) {
					// Exceptions at this point are fatal
					DatagramChannel channel = ensureChannel();
					setName(channel.socket().getLocalPort() + "-RequestListener");

					log.debug("{} started", getName());

					synchronized (this) {
						notifyAll();
					}

					try {
						while (true) {
							listen(selector);
						}
					} catch (SocketException e) {
						log.info("{}", e.getLocalizedMessage());
					} catch (IOException e) {
						log.error("RequestListener IO error ({})", e.getLocalizedMessage());
						log.debug("Failure trace", e);
					} finally {
						closeChannel();
					}
					log.debug("Reopening RequestListener in 100ms");
					sleep(100);
				}
			} catch (InterruptedException e) {
				log.debug("RequestListener was interrupted, closing");
			} catch (Throwable e) {
				log.error("RequestListener FAILED", e);
				closeChannel();
			}
		}

		private void listen(Selector selector) throws IOException {
			selector.select(500); // TODO timeout

			Iterator<SelectionKey> it = selector.selectedKeys().iterator();

			while (it.hasNext()) {
				SelectionKey key = it.next();

				it.remove();

				if (key.isReadable()) {
					DatagramChannel channel = (DatagramChannel) key.channel();
					try {
						inputBuffer.clear();
						SocketAddress address = channel.receive(inputBuffer);
						inputBuffer.flip();
						int len = inputBuffer.getInt();
						inputBuffer.limit(len);

						ObjectInputStream ois = new ObjectInputStream(new ByteBufferInputStream(
								inputBuffer));
						Message message = (Message) ois.readObject();
						process(address, message);

						System.err.println("RECV: " + message + " from: " + address);
					} catch (ClassNotFoundException e) {
						log.error(e.getLocalizedMessage());
					} catch (ClassCastException e) {
						log.error("Unsupported message type", e);
					} catch (IOException e) {
						log.error("Failed to receive message", e);
					}
				}
			}
		}
	}

	final class ResponseWriter extends Thread {
		private ByteBuffer outputBuffer;

		public ResponseWriter() {
			setName("ResponseWriter");
			setDaemon(true);

			this.outputBuffer = ByteBuffer.allocateDirect(socketBufferSize);
		}

		@Override
		public void run() {
			try {
				setName(getBoundAddress().getPort() + "-ResponseWriter");
				log.debug("{} started", getName());

				while (!isInterrupted()) {
					TransportMessage message = output.take();

					try {
						// Serializing message to buffer
						outputBuffer.clear();
						outputBuffer.position(4); // Reserving some space for the size
						ObjectOutputStream oos = new ObjectOutputStream(new ByteBufferOutputStream(
								outputBuffer));
						oos.writeObject(message.message);
						oos.flush();
						outputBuffer.flip();
						int len = outputBuffer.limit();
						outputBuffer.putInt(0, len); // Writing size field

						// Trying to write to the channel
						// TODO datagram size tests
						ensureChannel().send(outputBuffer, message.address);
						System.err.println("SENT: " + message.message + " to: " + message.address);
					} catch (IOException e) {
						log.error("Failed to write message", e);
						outputBuffer.clear();
						output.offer(message);
						log.debug("Reopening RequestWriter in 100ms");
						sleep(100);
					}
				}
			} catch (InterruptedException e) {
				log.debug("ResponseWriter was interrupted");
			} catch (Throwable e) {
				log.error("ResponseWriter FAILED", e);
			}
		}
	}

	/**
	 * Message callback for handling messages synchronously.
	 * 
	 * @author Gergely Kiss
	 * @param <R>
	 */
	private final class SyncMessageCallback<R> implements MessageCallback<R> {
		private final Message request;

		private boolean finished = false;
		private R response;
		private Throwable error;

		public SyncMessageCallback(Message request) {
			this.request = request;
		}

		@Override
		public void onSuccess(R response) {
			synchronized (request) {
				this.response = response;
				this.finished = true;
				request.notifyAll();
			}
		}

		@Override
		public void onFailure(Throwable error) {
			synchronized (request) {
				this.error = error;
				this.finished = true;
				request.notifyAll();
			}
		}

		public R waitUntil(long until) throws RemoteException, InterruptedException {
			synchronized (request) {
				while (!finished) {
					try {
						ensureChannel();
						System.err.println("Channel OK");
					} catch (IOException e) {
						throw new InterruptedException("Read failed: " + e.getLocalizedMessage());
					}
					ProcessUtils.check(until, "Read timed out");
					request.wait(100);
				}

				if (error != null) {
					throw new RemoteException(error);
				} else {
					return response;
				}
			}
		}
	}
}
