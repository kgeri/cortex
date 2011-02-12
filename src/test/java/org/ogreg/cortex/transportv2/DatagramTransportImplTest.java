package org.ogreg.cortex.transportv2;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.ogreg.cortex.RemoteException;
import org.ogreg.cortex.message.Invocation;
import org.ogreg.cortex.message.Message;
import org.ogreg.cortex.message.MessageCallback;
import org.ogreg.cortex.registry.ServiceRegistry;
import org.ogreg.cortex.registry.ServiceRegistryImpl;
import org.ogreg.cortex.transportv2.DatagramTransportImpl.TransportServer;
import org.ogreg.cortex.util.ProcessUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "correctness")
public class DatagramTransportImplTest {
	private List<Closeable> testConnections = new LinkedList<Closeable>();
	ServiceRegistry registry = new ServiceRegistryImpl();

	@BeforeMethod
	public void setUp() {
		registry.clear();
		Runtime.getRuntime().gc();
	}

	/**
	 * Tests simple connection opening and the InvokeMessage.
	 */
	@Test(timeOut = 1000)
	public void testInvokeMessage() throws Throwable {
		registry.register(new TestService(), null);
		registry.register(new TestService(new OutOfMemoryError("ONLY A TEST")),
				"bogus");

		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = open();

		SocketAddress address = conn2.getBoundAddress();

		Object r;

		// First request OK
		r = conn1.callSync(address,
				invoke(TestService.class, "add", "123", "456"), 10000);
		assertEquals(r, "123456");

		// Similar second request OK
		r = conn1.callSync(address,
				invoke(TestService.class, "add", "abc", "def"), 10000);
		assertEquals(r, "abcdef");

		// void request OK
		r = conn1.callSync(address, invoke(TestService.class, "noop"), 10000);
		assertEquals(r, null);

		// Invoking an unknown service
		try {
			r = conn1.callSync(address, invoke(String.class, "length"), 10000);
			fail("Expected ServiceException");
		} catch (RemoteException e) {
		}

		// Invoking an unknown method
		// TODO

		// Invoking an identified, bogus service
		try {
			r = conn1.callSync(address,
					invoke("bogus", TestService.class, "noop"), 10000);
			fail("Expected RemoteException");
		} catch (RemoteException e) {
			assertTrue(e.getCause() instanceof OutOfMemoryError,
					"Expected OutOfMemoryError cause");
		}
	}

	/**
	 * Tests connection opening errors.
	 */
	@Test(timeOut = 1000)
	public void testOpenErrors() throws Throwable {

		// Bind failed, all ports are in use
		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = create();
		
		try {
			conn2.setMinPort(conn1.getBoundAddress().getPort());
			conn2.setMaxPort(conn1.getBoundAddress().getPort());
			conn2.open(10000);
			fail("Expected TransportException");
		} catch (TransportException e) {
		}
		
		assertFalse(conn2.server.isAlive());

		// Other IO error occurred
		// This is hard to simulate
	}

	/**
	 * Tests asynchronous message passing.
	 */
	@Test(timeOut = 1000)
	public void testAsyncMessage() throws Exception {
		registry.register(new TestService(), null);
		registry.register(new TestService(new IllegalArgumentException()),
				"svc2");

		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = open();

		SocketAddress address = conn2.getBoundAddress();

		// Async invoke TODO
		Invocation message = invoke(TestService.class, "add", "123", "456");
		final AtomicBoolean success = new AtomicBoolean(false);
		MessageCallback<String> callback = new MessageCallback<String>() {
			@Override
			public void onSuccess(String response) {
				success.set("123456".equals(response));
			}

			@Override
			public void onFailure(Throwable error) {
				success.set(false);
			}
		};
		conn1.callAsync(address, message, callback);
		Thread.sleep(100);
		assertTrue(success.get());

		// DontCare invoke - the service may fail, we don't know about it
		message = invoke("svc2", TestService.class, "add", "abc", "def");
		conn1.callAsync(address, message);
		Thread.sleep(100);
	}

	/**
	 * Tests connection closing and opening, double closing and opening.
	 */
	@Test(timeOut = 1000)
	public void testOpenClose() throws Throwable {
		registry.register(new TestService(), null);

		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = open();

		SocketAddress address = conn2.getBoundAddress();

		Object r;

		// First request OK
		r = conn1.callSync(address,
				invoke(TestService.class, "add", "123", "456"), 10000);
		assertEquals(r, "123456");

		// Closing and reopening
		conn1.close();
		conn1.close();
		conn2.close();
		conn1.open(1000);
		conn2.open(1000);
		conn2.open(1000);

		address = conn2.getBoundAddress();

		// Second request OK
		r = conn1.callSync(address,
				invoke(TestService.class, "add", "abc", "def"), 10000);
		assertEquals(r, "abcdef");

		// Closing by finalizer
		TransportServer tmp = conn1.server;
		conn1.finalize();
		Thread.sleep(100);
		assertFalse(tmp.isAlive());
	}

	/**
	 * Tests a request timeout.
	 */
	@Test(timeOut = 1000)
	public void testTimeOuts() throws Throwable {
		registry.register(new TestService(100), null);

		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = open();

		SocketAddress address = conn2.getBoundAddress();

		// Method invoke timeout should result in an InterruptedException on the
		// client side
		try {
			conn1.callSync(address, invoke(TestService.class, "noop"), 50);
			fail("Expected InterruptedException");
		} catch (InterruptedException e) {
		}
	}
	
	/**
	 * Tests connection failure recovery.
	 */
	@Test(timeOut = 1000)
	public void testConnectionRecovery() throws Throwable {
		registry.register(new TestService(100), null);

		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = open();

		SocketAddress address = conn2.getBoundAddress();
		Object r;
		
		// Connections should be reopened on next invoke if the local host
		// closed them
		networkFailure(conn1, 0);
		r = conn1.callSync(address, invoke(TestService.class, "add", "1", "2"),
				500);
		assertEquals(r, "12");

		// Connections should be reopened on next invoke if the remote host
		// closed them
		networkFailure(conn2, 200);
		r = conn1.callSync(address, invoke(TestService.class, "add", "1", "2"),
				500);
		assertEquals(r, "12");

		// TODO Connections should be retried on next invoke if the remote host does
		// not answer
//		networkFailure(conn2, 0);
//		r = conn1.callSync(address, invoke(TestService.class, "add", "1", "2"),
//				500);
//		assertEquals(r, "12");
	}
	
	/**
	 * Tests errors.
	 */
	@Test(timeOut = 1000)
	public void testErrors() throws Throwable {
		registry.register(new TestService(100), null);

		DatagramTransportImpl conn1 = open();
		DatagramTransportImpl conn2 = open();

		SocketAddress address = conn2.getBoundAddress();

		// Null message
		try {
			conn1.callSync(address, null, 1000);
			fail("Expected NullPointerException");
		} catch (NullPointerException e) {
		}

		// Unsupported message type
		try {
			conn1.callSync(address, new DummyMessage(), 1000);
			fail("Expected RemoteException");
		} catch (RemoteException e) {
			assertTrue(e.getCause() instanceof UnsupportedOperationException,
					"Expected UnsupportedOperationException cause");
		}
	}

	@AfterMethod
	public void tearDown() {
		System.out.println("TEARDOWN");
		for (Closeable conn : testConnections) {
			try {
				System.out.println("Closing: " + conn);
				conn.close();
			} catch (IOException e) {
			}
		}
		testConnections.clear();
	}

	DatagramTransportImpl open() throws IOException, InterruptedException {
		DatagramTransportImpl conn = create();
		conn.open(5000);
		return conn;
	}

	DatagramTransportImpl create() {
		DatagramTransportImpl conn = new DatagramTransportImpl();
		testConnections.add(conn);
		conn.setRegistry(registry);
		conn.setSocketBufferSize(32768);
		conn.init();
		return conn;
	}

	Invocation invoke(Class<?> type, String methodName, Object... args)
			throws Exception {
		Class<?>[] types = new Class<?>[args.length];
		for (int i = 0; i < args.length; i++) {
			types[i] = args[i].getClass();
		}

		return Invocation.create(type.getDeclaredMethod(methodName, types),
				args);
	}

	Invocation invoke(String identifier, Class<?> type, String methodName,
			Object... args) throws Exception {
		Class<?>[] types = new Class<?>[args.length];
		for (int i = 0; i < args.length; i++) {
			types[i] = args[i].getClass();
		}

		return Invocation.create(identifier,
				type.getDeclaredMethod(methodName, types), args);
	}

	void networkFailure(DatagramTransportImpl transport, int sleep)
			throws InterruptedException {
		ProcessUtils.closeQuietly(transport.channel);
		System.out.println("TEST NETWORK FAILURE: " + transport);
		Thread.sleep(sleep);
	}
}

class TestService {
	private int responseTime = 0;
	private Throwable error = null;

	public TestService() {
		this(0);
	}

	public TestService(Throwable error) {
		this.error = error;
	}

	public TestService(int responseTime) {
		this.responseTime = responseTime;
	}

	// Adds the two strings
	public String add(String a, String b) throws Throwable {
		noop();
		return a + b;
	}

	// Does nothing but may fail :)
	public void noop() throws Throwable {
		Thread.sleep(responseTime);
		if (error != null) {
			throw error;
		}
	}
}

class DummyMessage extends Message {
	private static final long serialVersionUID = -280192501148489243L;
}