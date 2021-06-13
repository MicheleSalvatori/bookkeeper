package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.bookie.storage.ldb.ReadCache;
import org.junit.After;
import org.junit.AfterClass;
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
public class ReadCacheTest {
	private static ReadCache cache;
	private static ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
	private static long maxCacheSize = 10 * 1024;

	private long ledgerId;
	private long entryId;
	private ByteBuf entry;
	private int expected;
	private Class<? extends Exception> expectedException;

	public ReadCacheTest(long ledgerId, long entryId, ByteBuf entry, int expected, Class<? extends Exception> exception) {
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
		cache = new ReadCache(allocator, maxCacheSize);
		System.out.println("CONFIGURE");
	}

	@Parameters
	public static Collection<Object[]> data() {
		ByteBuf entryTest = Unpooled.wrappedBuffer(new byte[1024]);
		ByteBuf invalidEntryTest = Unpooled.wrappedBuffer(new byte[100 * 1024]);

		// {ledgerId, entryId, entry, expectedEntriesNumber, exception}
		return Arrays.asList(new Object[][] { { 0, 1, null, 0, NullPointerException.class },
				{ 0, 1, entryTest, 1, null }, 
				{ -1, 1, entryTest, 1, IllegalArgumentException.class }, // Non passa get perchè non viene scritta
				{ 0, 1, invalidEntryTest, 1, IndexOutOfBoundsException.class } });
	}

	@Test
	public void putTest() throws IOException {
		System.out.println("------PUT--------");
		System.out.println("LedgerID: " + ledgerId + " entryID: " + entryId);
		System.out.println("Entry: " + entry);

		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}

		cache.put(ledgerId, entryId, entry);
		assertEquals(cache.count(), expected);
	}

	@Test
	public void getTest() {
		System.out.println("------GET--------");
		 if(expectedException != null) {
	        	exceptionRule.expect(expectedException);
	        }
			cache.put(ledgerId, entryId, entry);								// Con il Before viene istanziata una nuova cache 
			System.out.println("LedgerID: "+ledgerId+ " entryID: "+entryId);
			System.out.println("Entry: "+entry);
			System.out.println("CacheGet: "+cache.get(ledgerId, entryId));
			assertEquals(entry, cache.get(ledgerId, entryId));
	}

			
	@After
	public void cleanUp() {
		cache.close();
	}

}
