package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

@RunWith(value = Parameterized.class)
public class AttemptTest {
	private ReadCache cache;
	private static ByteBufAllocator allocator;

	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private int expected;
	private Class<? extends Exception> expectedException;

	public AttemptTest(long ledgerId, long entryId, ByteBuf entry, int expected, Class<? extends Exception> exception) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entry;
		this.expected = expected;
		this.expectedException = exception;
	}

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	@Before
	public void configure() {
		allocator = ByteBufAllocator.DEFAULT;
		long maxCacheSize = 10 * 1024;
		cache = new ReadCache(allocator, maxCacheSize);
	}

	@Parameters
	public static Collection<Object[]> data() {
		ByteBuf entryTest = Unpooled.wrappedBuffer(new byte[1024]);
		// invalidEntry = dimensione maggiore a maxCacheSize

		return Arrays.asList(new Object[][] { { 0, 1, null, 1, NullPointerException.class },
				{ 0, 1, entryTest, 1, null }, { -1, 1, entryTest, 1, IllegalArgumentException.class } });
	}

	@Test
	public void putTest() throws IOException {

		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}

		cache.put(ledgerId, entryId, entry);

		assertEquals(cache.count(), expected);
	}

}
