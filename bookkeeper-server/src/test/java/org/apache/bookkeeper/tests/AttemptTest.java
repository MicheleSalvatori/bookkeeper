package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
	
	public AttemptTest(long ledgerId, long entryId, ByteBuf entry, int expected) {
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.entry = entry;
		this.expected = expected;
	}
	
	@Before
	public void configure() {
		allocator = ByteBufAllocator.DEFAULT;
		long maxCacheSize = 10 * 1024;
		cache = new ReadCache(allocator, maxCacheSize);
	}

	@Parameters
	public static Collection<Object[]> data() {
		ByteBuf entryTest = Unpooled.wrappedBuffer(new byte[1024]);
		return Arrays.asList(new Object[][] { { 1, 1, entryTest, 1}});
	}

	@Test
	public void putTest() throws IOException {
		cache.put(ledgerId, entryId, entry);
		assertEquals(cache.count(), 1);
	}

	/*
	@Test
	public void getTest() {
		cache.put(1, 0, entry);
		ByteBuf expectedValue = entry;
		ByteBuf actualValue = cache.get(1, 0);
		assertEquals(actualValue, expectedValue);
	}
	*/

}
