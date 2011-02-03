package org.ogreg.cortex.transport;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SocketBuilder {
	private static final Logger log = LoggerFactory.getLogger(SocketBuilder.class);

	private int minPort;
	private int maxPort;
	private int bufferSize = 1024;

	private int port = 0;

	public synchronized ServerSocket bind() throws BindException, IOException {
		ServerSocket socket = new ServerSocket();
		socket.setReceiveBufferSize(bufferSize);

		if (minPort > maxPort) {
			throw new IllegalArgumentException("minPort (" + minPort
					+ ") should be less than or equal to maxPort (" + maxPort + ")");
		}

		int startPort = Math.max(port, minPort);
		port = startPort;

		do {
			try {
				InetSocketAddress address = new InetSocketAddress(port);
				socket.bind(address);
				return socket;
			} catch (BindException e) {
				log.warn("{}: {}, retrying", e.getLocalizedMessage(), port);
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

	public SocketBuilder minPort(int minPort) {
		this.minPort = minPort;
		return this;
	}

	public SocketBuilder maxPort(int maxPort) {
		this.maxPort = maxPort;
		return this;
	}

	public SocketBuilder bufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}
}
