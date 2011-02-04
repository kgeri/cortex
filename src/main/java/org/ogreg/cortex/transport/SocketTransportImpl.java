package org.ogreg.cortex.transport;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.Iterator;
import java.util.Map;
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
import org.ogreg.cortex.util.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection manager for opening, closing and pooling standard (blocking) socket connections
 * towards other members of the cortex, and binding this node's server socket.
 * 
 * @author Gergely Kiss
 */
public class SocketTransportImpl implements Transport {
	static final Logger log = LoggerFactory.getLogger(SocketTransportImpl.class);

	/** The start of the usable port range (inclusive). */
	private int minPort = 4000;

	/** The end of the usable port range (inclusive). */
	private int maxPort = 4100;

	/** The size of the socket buffers. Default: 32768. */
	int socketBufferSize = 32768;

	/** The service registry used to service requests. */
	private ServiceRegistry registry;

	/** The opened channels towards the remote hosts. */
	final Map<String, ClientChannel> channels = new ConcurrentHashMap<String, ClientChannel>(32);

	/** Parallel executor service for processing incoming requests. */
	ExecutorService executor;

	RequestListener listener;

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
	public synchronized void open(long timeOut) throws InterruptedException {
		if (listener != null && listener.isAlive()) {
			return;
		}

		long until = System.currentTimeMillis() + timeOut;
		SocketBuilder sb = new SocketBuilder().minPort(minPort).maxPort(maxPort)
				.bufferSize(socketBufferSize);
		listener = new RequestListener(sb);

		synchronized (listener) {
			listener.start();
			listener.wait(ProcessUtils.check(until, "RequestListener open timed out"));
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

		for (Iterator<ClientChannel> it = channels.values().iterator(); it.hasNext();) {
			ClientChannel channel = it.next();
			channel.destroy();
			it.remove();
		}

		listener = null;
		log.info("Socket transport closed");
	}

	@Override
	public Object callSync(SocketAddress address, Message message, long timeOut)
			throws InterruptedException, RemoteException {
		long until = System.currentTimeMillis() + timeOut;

		ClientChannel channel = getChannel(address, null, until);
		SyncMessageCallback<Object> callback = new SyncMessageCallback<Object>(address, message);
		channel.offer(message, callback);
		return callback.waitUntil(until);
	}

	@Override
	public <R> void callAsync(SocketAddress address, Message message, long timeOut,
			MessageCallback<R> callback) throws InterruptedException {
		long until = System.currentTimeMillis() + timeOut;
		ClientChannel conn = getChannel(address, null, until);
		conn.offer(message, callback);
	}

	@Override
	public void callAsync(SocketAddress address, Message message, long timeOut)
			throws InterruptedException {
		long until = System.currentTimeMillis() + timeOut;
		ClientChannel conn = getChannel(address, null, until);
		conn.offer(message, null);
	}

	protected void finalize() throws Throwable {
		close();
	}

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
	void execute(SocketAddress source, Message message) {
		Response rsp;

		// TODO Use executor
		if (message instanceof Invocation) {
			Invocation m = (Invocation) message;

			try {
				Object service = registry.getService(m.getType(), m.getIdentifier());
				rsp = new Response(message.messageId, m.invoke(service));
			} catch (InvocationTargetException e) {
				rsp = new ErrorResponse(message.messageId, e.getCause());
			} catch (Exception e) {
				rsp = new ErrorResponse(message.messageId, e);
			}
		} else {
			rsp = new ErrorResponse(message.messageId, new UnsupportedOperationException(
					"Unsupported message: " + message));
		}

		// Sending response
		try {
			// TODO timeout
			getChannel(source, null, System.currentTimeMillis() + 1000).offer(rsp, null);
		} catch (InterruptedException e) {
			// If this happens, the channel is killed anyway, so we won't propagate the error back
			// to the reader thread
		}
	}

	private ClientChannel getChannel(SocketAddress address, Socket socket, long until)
			throws InterruptedException {
		String addr = address.toString().intern();

		synchronized (addr) {
			ClientChannel channel = channels.get(addr);

			try {
				if (channel != null) {
					return channel.ensureOpen(socket, until);
				}

				channel = new ClientChannelImpl(this, address);
				channel.ensureOpen(socket, until);
				channels.put(addr, channel);

				return channel;
			} catch (InterruptedException e) {
				channel.destroy();
				throw e;
			}
		}
	}

	private void registerChannel(Socket socket, long until) {
		SocketAddress address;

		try {
			// Incoming host must first identify itself
			ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
			address = (SocketAddress) ois.readObject();
			log.debug("Received connection from: {}", address);

			getChannel(address, socket, until);
		} catch (ClassCastException e) {
			log.error("Failed to accept connection from '" + socket.getRemoteSocketAddress()
					+ "', unexpected message: {}", e.getLocalizedMessage());
			ProcessUtils.closeQuietly(socket);
		} catch (IOException e) {
			log.error("Failed to accept connection from '" + socket.getRemoteSocketAddress()
					+ "', IO error while reading header: {}", e.getLocalizedMessage());
			ProcessUtils.closeQuietly(socket);
		} catch (Exception e) {
			log.error(
					"Unexpected failure while accepting connection from '"
							+ socket.getRemoteSocketAddress() + "'", e);
			ProcessUtils.closeQuietly(socket);
		}
	}

	/**
	 * Returns the address on which the connector is listening, or null if it is not.
	 * 
	 * @return
	 */
	public SocketAddress getBoundAddress() {
		return listener == null ? null : listener.getBoundAddress();
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

	// Server listener thread
	final class RequestListener extends Thread implements Closeable {
		private final SocketBuilder builder;
		ServerSocket server;

		public RequestListener(SocketBuilder builder) {
			this.builder = builder;
			setDaemon(true);
		}

		public void run() {
			try {
				while (!isInterrupted()) {
					// Exceptions at this point are fatal
					server = builder.bind();
					setName(server.getLocalPort() + "-RequestListener");
					log.debug("{} started", getName());

					synchronized (this) {
						notifyAll();
					}

					try {
						while (true) {
							Socket socket = server.accept();
							socket.setReceiveBufferSize(socketBufferSize);
							socket.setSendBufferSize(socketBufferSize);
							socket.setSoTimeout(5000); // TODO timeout

							// TODO timeout
							registerChannel(socket, System.currentTimeMillis() + 5000);
						}
					} catch (SocketException e) {
						log.info("{}", e.getLocalizedMessage());
					} catch (IOException e) {
						log.error("RequestListener IO error ({})", e.getLocalizedMessage());
						log.debug("Failure trace", e);
					}
					log.debug("Reopening RequestListener in 100ms");
					sleep(100);
				}
			} catch (InterruptedException e) {
				log.debug("RequestListener was interrupted, closing");
			} catch (Throwable e) {
				log.error("RequestListener FAILED", e);
			} finally {
				ProcessUtils.closeQuietly(this);
			}
		}

		@Override
		public void close() throws IOException {
			if (server != null) {
				server.close();
			}
		}

		SocketAddress getBoundAddress() {
			return server == null ? null : server.getLocalSocketAddress();
		}
	}

	/**
	 * Message callback for handling messages synchronously.
	 * 
	 * @author Gergely Kiss
	 * @param <R>
	 */
	private final class SyncMessageCallback<R> implements MessageCallback<R> {
		private final SocketAddress address;
		private final Message request;

		private boolean finished = false;
		private R response;
		private Throwable error;

		public SyncMessageCallback(SocketAddress address, Message request) {
			this.address = address;
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
					getChannel(address, null, until);
					ProcessUtils.check(until, "Read timed out");
					System.err.println("Waiting for " + address);
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
