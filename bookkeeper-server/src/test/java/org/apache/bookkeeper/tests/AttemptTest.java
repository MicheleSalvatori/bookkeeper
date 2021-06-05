package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.bookkeeper.bookie.EntryKey;
import org.apache.bookkeeper.bookie.SortedLedgerStorage;
import org.junit.Test;

public class AttemptTest {

	 @Test
	    public void dummyTest() throws IOException {
	        SortedLedgerStorage srl = new SortedLedgerStorage();
	        System.out.println(srl);
	        Long id = 4000L;
//	        ByteBuf ledger = LedgerDescriptor.createLedgerFenceEntry(id);
	        EntryKey entry = new EntryKey();
	        System.out.println(entry);
	        
	        int parameter = 5;
	        assertEquals(5, parameter);
	    }
}


