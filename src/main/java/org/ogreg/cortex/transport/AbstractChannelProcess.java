package org.ogreg.cortex.transport;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

import org.ogreg.cortex.util.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common base class for connection R/W threads.
 * 
 * @author Gergely Kiss
 */
abstract class AbstractChannelProcess extends Thread implements Closeable {
	private static final Logger log = LoggerFactory.getLogger(AbstractChannelProcess.class);

	private volatile boolean open = false;
	private final Socket socket;
	private ClientChannel channel;

	public AbstractChannelProcess(ClientChannel channel, Socket socket) {
		this.channel = channel;
		this.socket = socket;
	}

	public Socket getSocket() {
		return socket;
	}

	public boolean isOpen() {
		return open;
	}

	public synchronized void open(long until) throws InterruptedException {
		start();
		wait(ProcessUtils.check(until, "Timed out before " + getName() + " start"));
		ProcessUtils.check(until, getName() + " open timed out");
	}

	@Override
	public synchronized void close() {
		open = false;
		channel = null;
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
			log.debug(getName() + " interrupted");
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

	protected ClientChannel ensureAttached() throws IOException {
		if (channel == null) {
			throw new IOException(getName() + " is detached");
		}
		return channel;
	}
}