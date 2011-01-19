package org.ogreg.cortex.transport;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class InputStreamWrapper extends InputStream {
	private final InputStream is;
	private FileOutputStream os;

	public InputStreamWrapper(InputStream is, String file) {
		this.is = is;
		try {
			this.os = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int read() throws IOException {
		int b = is.read();
		os.write(b);
		return b;
	}

	@Override
	public int read(byte[] b) throws IOException {
		int len = is.read(b);
		os.write(b, 0, len);
		return len;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		len = is.read(b, off, len);
		os.write(b, off, len);
		return len;
	}
}
