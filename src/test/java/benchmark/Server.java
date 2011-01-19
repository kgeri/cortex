package benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

class Server extends Thread {
	private ServerSocket socket;

	public Server() {
		setDaemon(true);
	}

	@Override
	public void run() {
		try {
			socket = new ServerSocket();
			socket.setReuseAddress(true);
			socket.bind(new InetSocketAddress(4000));

			synchronized (this) {
				notify();
			}

			while (!isInterrupted()) {
				Socket sock = socket.accept();
				InputStream is = sock.getInputStream();
				byte[] buf = new byte[1024];

				while (is.read(buf) >= 0) {
				}
			}
		} catch (SocketException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void interrupt() {
		super.interrupt();
		try {
			socket.close();
		} catch (IOException e) {
		}
	}
}