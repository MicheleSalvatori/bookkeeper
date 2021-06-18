package org.apache.bookkeeper.bookie;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;
import org.mockito.Spy;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.spy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

@RunWith(value = Parameterized.class)					// non si possono usare due runner insieme
public class InternalReadEntryTest extends BookKeeperClusterTestCase{
	
	private static int numBookies = 3;
	private EntryLogger entryLogger;
	private LedgerHandle handle;
	
	private long ledgerId;
	private long entryId;
	private long location;
	private boolean validateEntry;
	private boolean entryExist;
	private Class<? extends Exception> expectedException;
	
	private long position;				// position of the entry in the entryLog
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	public InternalReadEntryTest(long ledgerId, long entryId, long location, boolean validateEntry, boolean entryExist, Class<? extends Exception> expectedException) {
		super(numBookies);
		this.ledgerId = ledgerId;
		this.entryId = entryId;
		this.location = location;
		this.validateEntry = validateEntry;
		this.entryExist = entryExist;
		this.expectedException = expectedException;
	}
	
	@Before	// Non posso utilizzare il BeforeClass perchè essendo static non potrei riferirmi a super.setUp()
	public void setUp() throws Exception{				// da problemi con il nome configure, c'è un altro metodo configure in ServerCnxnFactory.java
		super.setUp();					// Inizializzaione cluester BookKeeper e ZookKeeper
		entryLogger = new EntryLogger(baseConf);	
		/**
		 * Ledger handle contains ledger metadata and is used to access the read and
		 * write operations to a ledger.
		 * 
		 * Creates a new ledger. Default of 3 servers, and quorum of 2 servers.
		 * Return an handle to the ledger created
		 */
		
		if (entryExist) {
			handle = super.bkc.createLedger(BookKeeper.DigestType.CRC32, "testPasswd".getBytes());		// Creazione ledger possibile attraverso un ledgerHandle
			ByteBuf entry = Unpooled.buffer(1024);
			entry.writeLong(handle.getId());															// Inserisce il ledgerId nell'entry
			entry.writeLong(entryId);																	// Inserisce l'entryId nell'entry
			entry.writeBytes("bytesToRead".getBytes());
			position = entryLogger.addEntry(handle.getId(), entry, true);					// Ritorna l'effettiva posizione dell'entry nell'entryLog da cui poi dobbiamo leggerla
			if (location == 500L) {								// simulate bad position given
//				position = location;								// Represents case where entry log is present, but does not contain the specified entry (tentiamo di leggere su una posizione sbagliata dell log)
				System.out.println("POSITION: "+position);
				System.out.println("LOCATION: "+location);
			}
		}else {
			position = location;
		}
	}
	
	@After
	public void tearDown() throws Exception  {
		entryLogger.shutdown();
		super.tearDown();
	}
	
	@Parameters
	public static Collection<?> getParameters() {

		return Arrays.asList(new Object[][] {
			// Test suite minimale
//			{-1L, -1L, -1L, true, false, IOException.class},
//			{1L, 1L, 0L, false, true, null},
//			{0L, 0L, 1L, true, true, null},
//			
//			// Added for jacoco
			{1L,500L, 500L, false, true, Bookie.NoEntryException.class}		
			
		});
	}
	
	@Test
	public void testReadEntry() throws IOException {
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		
		ByteBuf readedBuf = entryLogger.internalReadEntry(handle.getId(), entryId, position, validateEntry);
		byte[] checkBuf = new byte[readedBuf.capacity()];
		
		readedBuf.getBytes(0, checkBuf);
		String readedString = new String(checkBuf);
		System.out.println(readedString);
		assertTrue(readedString.contains("bytesToRead"));
		
	}

}
