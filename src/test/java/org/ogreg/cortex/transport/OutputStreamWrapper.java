package org.ogreg.cortex.transport;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamWrapper extends OutputStream {
	private final OutputStream s;
	private FileOutputStream os;

	public OutputStreamWrapper(OutputStream s, String file) {
		this.s = s;
		try {
			this.os = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(int b) throws IOException {
		s.write(b);
		os.write(b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		s.write(b);
		os.write(b);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		s.write(b, off, len);
		os.write(b, off, len);
	}
}
