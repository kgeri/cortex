package org.ogreg.cortex.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class ByteBufferInputStream extends InputStream {
	private final ByteBuffer buffer;

	public ByteBufferInputStream(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	@Override
	public int read() throws IOException {
		return buffer.hasRemaining() ? buffer.get() : -1;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		len = Math.min(len, buffer.remaining());
		buffer.get(b, off, len);
		return len;
	}
}
