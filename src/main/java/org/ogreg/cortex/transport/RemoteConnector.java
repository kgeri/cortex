package org.ogreg.cortex.transport;

 import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketTimeoutException;

import javax.annotation.PostConstruct;

import org.ogreg.cortex.registry.ServiceRegistry;
import org.ogreg.cortex.transport.MulticastProtocol.Messages;
import org.ogreg.cortex.transport.MulticastProtocol.Messages.Discover;
import org.ogreg.cortex.transport.MulticastProtocol.Messages.ServiceLoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteConnector implements Closeable {
	static final Logger log = LoggerFactory.getLogger(RemoteConnector.class);

	static final int DEFAULT_RECEIVE_BUFFER_SIZE = 65535;

	private String cortex;
	private InetAddress localhost;

	// Discovery attributes

	private int port = 1100;
	private String address = "230.0.0.0";
	private int timeout = 3;
	private int timeToLive = 1;
	private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;

	private ServiceRegistry registry;
	private Transport connector;

	private transient MulticastSocket discovery;
	private transient Thread listener;

	public void send(String cortex, Serializable message) {
		// TODO Auto-generated method stub

	}

	@PostConstruct
	public synchronized void open() throws IOException {
		if (cortex == null) {
			throw new IllegalArgumentException(
					"Cortex identifier must be specified prior to opening a Connector");
		}

		// TODO Separate interfaces?
		localhost = InetAddress.getLocalHost();

		if (discovery != null) {
			close();
		}

		try {
			MulticastSocket s = new MulticastSocket(port);
			s.setSoTimeout(timeout * 1000);
			s.setTimeToLive(timeToLive);
			s.setLoopbackMode(true);
			s.setReceiveBufferSize(receiveBufferSize);
			s.joinGroup(InetAddress.getByName(address));
			discovery = s;

			listener = new MulticastListener(discovery);
			listener.start();
			log.debug("MulticastConnector started successfully");
		} catch (IOException e) {
			log.error("Failed to start MulticastConnector", e);
			close();
			throw e;
		}
	}

	@Override
	public synchronized void close() {
		if (listener != null) {
			listener.interrupt();
		}

		if (discovery != null) {
			discovery.close();
		}

		discovery = null;
		listener = null;

		log.debug("MulticastConnector closed");
	}

	public void setCortex(String cortex) {
		this.cortex = cortex;
	}

	private Messages processControlMessage(Messages msg, InetAddress source) {
		String cortex = msg.getCortex();

		if (!this.cortex.equals(cortex)) {
			log.trace("Skipping message (foreign cortex: {})", cortex);
			return null;
		}

		// Discovery request
		if (msg.hasDiscover()) {
			Discover m = msg.getDiscover();
			String identifier = m.hasIdentifier() ? m.getIdentifier() : "";
			String clazz = m.getClazz();

			if (registry.hasService(clazz, identifier)) {
				// Return service location response
				return Messages
						.newBuilder()
						.setServiceLocation(
								ServiceLoc.newBuilder().setClazz(clazz).setIdentifier(identifier))
						.build();
			} else {
				// No response when target is not registered
				return null;
			}
		}
		// ServiceLoc response
		else if (msg.hasServiceLocation()) {
			ServiceLoc m = msg.getServiceLocation();
			String identifier = m.hasIdentifier() ? m.getIdentifier() : "";
			String clazz = m.getClazz();

//			RemoteConnection remote = remoteConnectionMap.get(clazz + "#" + identifier);

			// if (remote != null && remote.ensureOpen()) {
			// }

			return null;
		} else {
			log.error("Unprocessable message: {}", msg.toString());
			return null;
		}
	}

	private final class MulticastListener extends Thread {
		private final MulticastSocket socket;

		public MulticastListener(MulticastSocket socket) {
			this.socket = socket;
			setName("MulticastListener");
			setDaemon(true);
		}

		public void run() {
			DatagramPacket request = new DatagramPacket(new byte[receiveBufferSize],
					receiveBufferSize);

			try {
				InetAddress address = InetAddress.getByName(RemoteConnector.this.address);

				while (!isInterrupted()) {
					Messages msg;

					try {
						socket.receive(request);

						byte[] data = request.getData();
						msg = Messages.parseFrom(data);
					} catch (SocketTimeoutException e) {
						log.error("Socket timeout! A datagram packet was lost");
						continue;
					}

					// Determining response
					Messages rsp = processControlMessage(msg, request.getAddress());

					if (rsp == null) {
						continue;
					}

					// Responding to the message
					byte[] data = rsp.toByteArray();
					DatagramPacket response = new DatagramPacket(data, data.length, address,
							RemoteConnector.this.port);

					socket.send(response);
				}
			} catch (Throwable e) {
				if (Thread.currentThread().isInterrupted()) {
					log.error("MulticastListener failed and is interrupted, exiting", e);
				} else {
					log.error("MulticastListener failed, trying to restart in 5 seconds...", e);

					try {
						sleep(5000);
						open();
					} catch (InterruptedException ie) {
						log.error("Sleep interrupted");
					} catch (IOException ioe) {
						log.error("Failed to restart MulticastListener. Giving up :(", e);
					}
				}
			}
		}
	}
}
