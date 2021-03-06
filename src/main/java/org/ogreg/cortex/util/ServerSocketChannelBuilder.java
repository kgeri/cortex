package org.ogreg.cortex.util;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.ServerSocketChannel;

import org.slf4j.Logger;

/**
 * A Builder for easing creation of {@link ServerSocket}s.
 * 
 * @author Gergely Kiss
 */
public class ServerSocketChannelBuilder {
	private Logger log = null;

	/** The first port to use when binding. */
	private int minPort;

	/** The last port to use when binding. */
	private int maxPort;

	/** The socket buffer size to use when binding. */
	private int bufferSize = 1024;

	/** The currently bound port, or 0 if not bound. */
	private int port = 0;

	public synchronized ServerSocketChannel bind() throws BindException, IOException {
		ServerSocketChannel channel = ServerSocketChannel.open();
		channel.socket().setReceiveBufferSize(bufferSize);
		channel.configureBlocking(false);

		if (minPort > maxPort) {
			throw new IllegalArgumentException("minPort (" + minPort
					+ ") should be less than or equal to maxPort (" + maxPort + ")");
		}

		int startPort = Math.max(port, minPort);
		port = startPort;

		do {
			try {
				InetSocketAddress address = new InetSocketAddress(port);
				channel.socket().bind(address);
				return channel;
			} catch (BindException e) {
				if (log != null) {
					log.warn("{}: {}, retrying", e.getLocalizedMessage(), port);
				}
			}

			port++;
			if (port > maxPort) {
				port = minPort;
			}
		} while (port != startPort);

		port = 0;
		throw new BindException("Port range already in use [" + minPort + "-" + maxPort
				+ "], giving up");
	}

	/**
	 * Specifies the first port to try when binding (inclusive). Must be &lt;= {@link #maxPort(int)}
	 * .
	 * 
	 * @param minPort
	 * @return
	 */
	public ServerSocketChannelBuilder minPort(int minPort) {
		this.minPort = minPort;
		return this;
	}

	/**
	 * Specifies the last port to try when binding (inclusive). Must be &gt;= {@link #minPort(int)}.
	 * 
	 * @param maxPort
	 * @return
	 */
	public ServerSocketChannelBuilder maxPort(int maxPort) {
		this.maxPort = maxPort;
		return this;
	}

	/**
	 * Specifies the socket buffer size to use.
	 * 
	 * @param bufferSize
	 * @return
	 */
	public ServerSocketChannelBuilder bufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	/**
	 * Specifies the logger to use for outputting binding messages.
	 * 
	 * @param log
	 * @return
	 */
	public ServerSocketChannelBuilder log(Logger log) {
		this.log = log;
		return this;
	}
}
