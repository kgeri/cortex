package org.ogreg.cortex.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection manager for opening, closing and pooling standard (blocking) socket connections
 * towards other members of the cortex, and binding this node's server socket.
 * 
 * @author Gergely Kiss
 */
public class SocketTransportImpl implements Transport {
	private static final Logger log = LoggerFactory.getLogger(SocketTransportImpl.class);

	/** The start of the usable port range (inclusive). */
	private int minPort = 4000;

	/** The end of the usable port range (inclusive). */
	private int maxPort = 4100;

	/** The size of the socket buffers. Default: 32768. */
	private int socketBufferSize = 32768;

	/** The port on which the server was bound, or 0 if it is not. */
	private int port = 0;

	/** The service registry used to service requests. */
	private ServiceRegistry registry;

	/** The opened channels towards the remote hosts. */
	// Package private for testing
	final Map<String, Connection> connections = new ConcurrentHashMap<String, Connection>(32);

	/** Parallel executor service for processing incoming requests. */
	// Package private for testing
	ExecutorService executor;

	private RequestListener listener;

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

	/**
	 * Binds the transport to one of the ports specified between {@link #minPort} and
	 * {@link #maxPort} inclusively and starts the listener process.
	 * 
	 * @throws BindException if simple bind failed
	 * @throws IOException on other connection error
	 */
	@Override
	public synchronized void open() throws IOException {
		if (port > 0) {
			return;
		}

		try {
			ServerSocket socket = new ServerSocket();
			socket.setReceiveBufferSize(socketBufferSize);

			for (port = minPort; port <= maxPort; port++) {
				try {
					InetSocketAddress address = new InetSocketAddress(port);
					socket.bind(address);
					log.info("Successfully bound to: {}", port);
					break;
				} catch (BindException e) {
					if (port < maxPort) {
						log.warn("{}: {}, retrying", e.getLocalizedMessage(), port);
					} else {
						port = 0;
						throw new BindException("Port range already in use [" + minPort + "-"
								+ maxPort + "], giving up");
					}
				}
			}

			listener = new RequestListener(socket);
			listener.start();
		} catch (BindException e) {
			throw e;
		} catch (IOException e) {
			port = 0;
			log.error("Failed to start connector", e);
			close();
			throw e;
		}
	}

	/**
	 * Stops listening and closes all server and client connections.
	 */
	@Override
	public synchronized void close() throws IOException {

		if (listener != null) {
			listener.interrupt();
		}
		
		closeAllConnections();

		listener = null;
		port = 0;

		log.info("Socket transport closed");
	}

	void closeAllConnections() {
		for (Iterator<Connection> it = connections.values().iterator(); it.hasNext();) {
			Connection connection = it.next();
			connection.close();
			it.remove();
		}
	}

	@Override
	public Object callSync(SocketAddress address, Message message, long timeOut)
			throws IOException, InterruptedException, RemoteException {
		long until = System.currentTimeMillis() + timeOut;
		Connection conn = getConnection(address, null, until);

		SyncMessageCallback<Object> callback = new SyncMessageCallback<Object>(conn, message);
		conn.append(message, callback);
		return callback.waitUntil(until);
	}

	@Override
	public <R> void callAsync(SocketAddress address, Message message, long timeOut,
			MessageCallback<R> callback) throws IOException, InterruptedException {
		long until = System.currentTimeMillis() + timeOut;
		Connection conn = getConnection(address, null, until);
		conn.append(message, callback);
	}

	@Override
	public void callAsync(SocketAddress address, Message message, long timeOut) throws IOException,
			InterruptedException {
		long until = System.currentTimeMillis() + timeOut;
		Connection conn = getConnection(address, null, until);
		conn.append(message, null);
	}

	protected void finalize() throws Throwable {
		close();
	}

	/**
	 * Executes <code>message</code>, and returns a response.
	 * <p>
	 * Should never throw an error, since it's called by the reader thread. On failures, this method
	 * should return {@link ErrorResponse}s.
	 * </p>
	 * 
	 * @param message
	 * @return
	 */
	private Message execute(Message message) {

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
		}

		return new ErrorResponse(message.messageId, new UnsupportedOperationException(
				"Unsupported message: " + message));
	}

	private Connection getConnection(SocketAddress address, Socket socket, long until)
			throws IOException, InterruptedException {
		String addr = address.toString().intern();

		synchronized (addr) {
			Connection connection = connections.get(addr);

			if (connection != null) {
				if (connection.isOpen() && (socket == null)) {
					return connection;
				} else {
					closeConnection(connection);
				}
			}

			if (socket == null) {
				socket = new Socket();
				socket.setKeepAlive(true);
				socket.setReuseAddress(true);
				socket.setReceiveBufferSize(socketBufferSize);
				socket.setSendBufferSize(socketBufferSize);
				socket.connect(address, (int) check(until, "Timed out before socket connect"));
				log.debug("Opened connection to: {}", addr);
			}

			try {
				connection = new Connection(addr, socket);
				connection.start(until);
				connections.put(addr, connection);

				return connection;
			} catch (InterruptedException e) {
				closeConnection(connection);
				throw e;
			}
		}
	}

	void closeConnection(Connection connection) {
		String addr = connection.getKey();
		
		synchronized (addr) {
			Connection conn = connections.get(addr);

			if (conn != null && conn == connection) {
				connections.remove(addr);
				log.debug("Removed and closing connection to: {}", addr);
				conn.close();
			}
		}
	}

	/**
	 * Returns the port on which the connector is listening, or 0 if it is not.
	 * 
	 * @return
	 */
	public int getBoundPort() {
		return port;
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

	private static long check(long until, String message) throws InterruptedException {
		if (Thread.interrupted()) {
			throw new InterruptedException();
		} else {
			long remaining = until - System.currentTimeMillis();
			if (remaining < 1) {
				throw new InterruptedException(message);
			} else {
				return remaining;
			}
		}
	}

	// Server listener thread
	private final class RequestListener extends Thread {
		private final ServerSocket server;

		public RequestListener(ServerSocket server) {
			this.server = server;
			setName(port + "-RequestListener");
			setDaemon(true);
		}

		public void run() {

			try {
				while (!isInterrupted()) {
					Socket socket = server.accept();
					socket.setReceiveBufferSize(socketBufferSize);
					socket.setSendBufferSize(socketBufferSize);

					SocketAddress address = socket.getRemoteSocketAddress();

					getConnection(address, socket, System.currentTimeMillis() + 5000);

					log.debug("Received connection from: {}", address);
				}
			} catch (IOException e) {
				log.error("RequestListener IO error ({})", e.getLocalizedMessage());
				log.debug("Failure trace", e);
			} catch (Throwable e) {
				log.error("RequestListener FAILED", e);
			} finally {
				interrupt();
			}
		}

		@Override
		public void interrupt() {
			super.interrupt();
			try {
				server.close();
			} catch (IOException e) {
			}
		}
	}

	/**
	 * Represents a duplex connection against a remote host.
	 * 
	 * @author Gergely Kiss
	 */
	// Package private for testing
	final class Connection {
		private final String addr;
		private final Socket socket;

		private final Queue<Message> output = new LinkedBlockingQueue<Message>();
		private final Semaphore semOutput = new Semaphore(0);
		private final Map<Integer, MessageCallback<?>> callbacks = new ConcurrentHashMap<Integer, MessageCallback<?>>();

		private final Thread reader;
		private final Thread writer;

		private boolean open = false;

		public Connection(String addr, Socket socket) {
			this.addr = addr;
			this.socket = socket;

			SocketAddress address = socket.getRemoteSocketAddress();

			reader = new ChannelReader();
			reader.setName(port + "-ChannelReader-" + address);
			reader.setDaemon(true);

			writer = new ChannelWriter();
			writer.setName(port + "-ChannelWriter-" + address);
			writer.setDaemon(true);
		}

		private void start(long until) throws InterruptedException {
			synchronized (writer) {
				writer.start();
				writer.wait(check(until, "Timed out before Reader start"));
			}
			synchronized (reader) {
				reader.start();
				reader.wait(check(until, "Timed out before Writer start"));
			}
			check(until, "Connection start timed out");
			this.open = true;
		}

		private <R> void append(Message message, MessageCallback<R> callback) throws IOException {
			if (callback != null) {
				callbacks.put(message.messageId, callback);
			}
			output.add(message);
			semOutput.release();
		}

		private boolean isOpen() {
			System.err.println(addr + " open=" + open);
			return open;
		}

		private void close() {
			open = false;
			try {
				socket.close();
			} catch (IOException e) {
				log.error("Failed to close socket: {} ({})", socket, e.getLocalizedMessage());
				log.debug("Failure trace", e);
			}
			reader.interrupt();
			writer.interrupt();

			for (MessageCallback<?> callback : callbacks.values()) {
				try {
					callback.onFailure(new IOException("Connection closed"));
				} catch (Exception e) {
					log.error("Failed to callback: " + callback, e);
				}
			}

			for (Message message : output) {
				synchronized (message) {
					message.notifyAll();
				}
			}

			log.debug("Connection closed, callbacks notified: " + addr);
		}

		String getKey() {
			return addr;
		}

		final class ChannelReader extends Thread {
			@Override
			@SuppressWarnings({ "unchecked", "rawtypes" })
			public void run() {
				try {
					InputStream in = socket.getInputStream();
					ObjectInputStream is = new ObjectInputStream(in);

					synchronized (this) {
						notifyAll();
					}

					while (!isInterrupted()) {
						// Responding to incoming messages
						Message message;

						try {
							message = (Message) is.readObject();
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
							Message response = execute(message);
							append(response, null);
						}
					}
				} catch (IOException e) {
					log.debug("ChannelReader IO error ({})", e.getLocalizedMessage());
					log.trace("Failure trace", e);
				} catch (Throwable e) {
					log.error("ChannelReader FAILED", e);
				} finally {
					closeConnection(Connection.this);
				}
			}

		}

		final class ChannelWriter extends Thread {
			@Override
			public void run() {
				try {
					OutputStream out = socket.getOutputStream();
					ObjectOutputStream os = new ObjectOutputStream(out);
					os.flush();

					synchronized (this) {
						notifyAll();
					}

					while (!isInterrupted()) {

						// Sending the next message from the queue
						semOutput.acquire();
						Message message = output.poll();
						if (message != null) {
							os.writeObject(message);
							os.reset();
							os.flush();
						}
					}
				} catch (InterruptedException e) {
					log.debug("ChannelWriter interrupted");
				} catch (IOException e) {
					log.debug("ChannelWriter IO error ({})", e.getLocalizedMessage());
					log.trace("Failure trace", e);
				} catch (Throwable e) {
					log.error("ChannelWriter FAILED, closing connection", e);
				} finally {
					closeConnection(Connection.this);
				}
			}
		}
	}

	/**
	 * Message callback for handling messages synchronously.
	 * 
	 * @author Gergely Kiss
	 * @param <R>
	 */
	private static final class SyncMessageCallback<R> implements MessageCallback<R> {
		private final Connection connection;
		private final Message request;

		private boolean finished = false;
		private R response;
		private Throwable error;

		public SyncMessageCallback(Connection connection, Message request) {
			this.connection = connection;
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

		public R waitUntil(long until) throws IOException, RemoteException, InterruptedException {
			synchronized (request) {
				while (!finished) {
					if (!connection.isOpen()) {
						throw new IOException("Connection was closed while waiting for response");
					}
					check(until, "Response timed out");
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
