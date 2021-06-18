package org.apache.bookkeeper.bookie;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith (value = Parameterized.class)
public class AddEntryForCompactionTest extends BookKeeperClusterTestCase {

	private static int numBookies = 3;
	private static String MESSAGE_TEST = "bytesToRead";
	private EntryLogger entryLogger;
	private LedgerHandle handle;
	
	private long ledgerId;
	private Boolean isValid;
	private Class<? extends Exception> expectedException;
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	public AddEntryForCompactionTest(long ledgerId, Boolean isValid, Class<? extends Exception> expectedException) {
		super(numBookies);
		this.ledgerId = ledgerId;
		this.isValid = isValid;
		this.expectedException = expectedException;
	}
	
	@Before
	public void setUp() throws Exception {
		super.setUp();
		baseConf.setOpenFileLimit(1);
		File test = new File("/tmp/bk-data/current");
	    test.mkdirs();
		entryLogger = new EntryLogger(baseConf);
		handle = bkc.createLedger(BookKeeper.DigestType.CRC32, "testPasswd".getBytes());
		
	}
	
	@After
	public void tearDown() throws Exception  {
//		super.tearDown();
	}
	
	@Parameters
	public static Collection<?> getParameters() {
		Boolean validEntry = new Boolean(true);
		Boolean notValidEntry = new Boolean(false);								// validateEntry non viene mai chiamata per verificare l'entry
		
		return Arrays.asList(new Object[][] {
			// Test suite minimale
			{1L, validEntry, null},
			{-1L, notValidEntry, IllegalArgumentException.class},   // Key and value must be >= 0
			{0L, null, NullPointerException.class},
			
			// Added for jacoco coverage
			{3L, validEntry, null}
		});
	}
	
	@Test
	public void testAddEntry() throws IOException {
		if(expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		
		ByteBuf entry = null;
		
		if (isValid != null) {
			if (isValid) {
				entry = Unpooled.buffer(1024);
				entry.writeLong(ledgerId);
				entry.writeLong(1);
				entry.writeBytes(MESSAGE_TEST.getBytes());
				if (ledgerId == 3L) {
					entryLogger.createNewCompactionLog();
				}
			}else {
				entry = Unpooled.buffer(6 * 1024 * 1024);				// no effect
			}
		}
		
		entryLogger.addEntryForCompaction(ledgerId, entry);
		entryLogger.flushCompactionLog();
		
		File file = entryLogger.getCurCompactionLogFile();
		byte[] logFile = Files.readAllBytes(file.toPath());
		String stringLogFile = new String(logFile);
		
		assertEquals(true, stringLogFile.contains(MESSAGE_TEST));
	}

}
