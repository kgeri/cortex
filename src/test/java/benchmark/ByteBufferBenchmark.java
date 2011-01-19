package benchmark;

import java.nio.ByteBuffer;

import org.testng.annotations.Test;

//Benchmark to prove why pooling ByteBuffers is worth the trouble
public class ByteBufferBenchmark {

	@Test
	public void testAllocateDirect() throws Exception {
		long before = System.currentTimeMillis();

		for (int i = 0; i < 1000; i++) {
			ByteBuffer.allocateDirect(0x100000); // 1M
		}

		System.err.println("1000 x 1M allocate: " + (System.currentTimeMillis() - before) + "ms");
	}

	@Test
	public void testCreateObject() throws Exception {
		long before = System.currentTimeMillis();

		for (int i = 0; i < 1000; i++) {
			new Object();
		}

		System.err.println("1000 new Object(): " + (System.currentTimeMillis() - before) + "ms");
	}
}
