package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

//@RunWith(value = Parameterized.class)
public class AttemptTest {
	private static ReadCache cache;
	private static ByteBuf entry;
	
	@Before
	public void configure() {
		ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
		long maxCacheSize = 10 * 1024;
		cache = new ReadCache(allocator, maxCacheSize);
		entry = Unpooled.wrappedBuffer(new byte[1024]);
	}

	@Test
	public void putTest() throws IOException {
		cache.put(1, 0, entry);
		assertEquals(cache.count(), 1);
	}
	
	@Test
	public void getTest() {
		cache.put(1, 0, entry);
		ByteBuf expectedValue = entry;
		ByteBuf actualValue = cache.get(1, 0);
		assertEquals(actualValue, expectedValue);
	}

}
