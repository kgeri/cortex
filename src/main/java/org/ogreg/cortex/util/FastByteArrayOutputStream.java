package org.ogreg.cortex.util;

import java.io.OutputStream;

/**
 * A fast, non-threadsafe byte output stream implementation.
 * 
 * @author Gergely Kiss
 */
public class FastByteArrayOutputStream extends OutputStream {

	/** The buffer where data is stored. */
	private byte contents[];

	/** The number of valid bytes in the buffer. */
	private int size;

	public FastByteArrayOutputStream() {
		this(4096);
	}

	public FastByteArrayOutputStream(int size) {
		this.contents = new byte[size];
		this.size = 0;
	}

	public void write(int b) {
		ensure(size);
		contents[size++] = (byte) b;
	}

	public void write(byte[] b, int off, int len) {
		ensure(size + len);
		System.arraycopy(b, off, contents, size, len);
		size += len;
	}

	/**
	 * Ensures that we have a large enough buffer for the given write position.
	 */
	private final void ensure(int position) {
		int len = contents.length;

		if (position < len) {
			return;
		}
		
		while (position >= len) {
			len <<= 1;
		}

		byte[] tmp = new byte[len];
		System.arraycopy(contents, 0, tmp, 0, contents.length);
		contents = tmp;
	}

	/**
	 * Returns the underlying byte buffer.
	 * <p>
	 * Warning: a reference to the buffer is returned, please also use {@link #size} to determine
	 * the number of used bytes.
	 * <p>
	 * 
	 * @return
	 */
	public byte[] getContents() {
		return contents;
	}

	/**
	 * Returns the number of bytes written to this buffer.
	 * 
	 * @return
	 */
	public int size() {
		return size;
	}
}
