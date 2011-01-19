package org.ogreg.cortex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cortex {
	private static final Logger log = LoggerFactory.getLogger(Cortex.class);

	public static void send(String cortex, String address, Object message) {
//		try {
//			FastByteArrayOutputStream baos = new FastByteArrayOutputStream(Connector.DEFAULT_RECEIVE_BUFFER_SIZE);
//			ObjectOutputStream oos = new ObjectOutputStream(baos);
//			oos.writeObject(message);
//			
//			Message msg = new Message(cortex, address, baos.getContents(), 0, baos.size());
//		} catch (IOException e) {
//			log.error("Failed to send message to {}:{}, created from object: {} ({})",
//					new Object[] { cortex, address, message, e.getLocalizedMessage() });
//			log.debug("Failure trace", e);
//		}
	}
}
