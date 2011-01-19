package benchmark;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

//Benchmark to prove why keep-alive is worth the trouble
public class TCPBenchmark {
	byte[] buf = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
			.getBytes();
	InetAddress localhost;
	Server server;

	@BeforeMethod
	public void setUp() throws Exception {
		localhost = InetAddress.getLocalHost();
		server = new Server();
		server.start();

		synchronized (server) {
			server.wait();
		}
	}

	@AfterMethod
	public void tearDown() {
		server.interrupt();
	}

	@Test
	public void testConnects() throws Exception {

		long before = System.currentTimeMillis();

		for (int i = 0; i < 1000; i++) {
			Socket socket = new Socket();
			socket.setReuseAddress(true);
			socket.setSoLinger(false, 0);
			socket.connect(new InetSocketAddress(localhost, 4000));
			OutputStream os = socket.getOutputStream();
			os.write(buf);
			os.flush();
			socket.close();
		}

		System.err.println("1000 x " + buf.length + "b send: "
				+ (System.currentTimeMillis() - before) + "ms");
	}

	@Test
	public void testStream() throws Exception {

		long before = System.currentTimeMillis();

		Socket socket = new Socket();
		socket.setReuseAddress(true);
		socket.setSoLinger(false, 0);
		socket.connect(new InetSocketAddress(localhost, 4000));
		OutputStream os = socket.getOutputStream();

		for (int i = 0; i < 1000; i++) {
			os.write(buf);
		}

		os.flush();
		socket.close();

		System.err.println(1000 * buf.length + "b send: " + (System.currentTimeMillis() - before)
				+ "ms");
	}
}
