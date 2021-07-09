package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.BookieException.BookieIllegalOpException;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.protobuf.ByteString;

@RunWith(value = Parameterized.class)
public class SetMasterKeyTest {

	private LedgerMetadataIndex ledgerMetadataIndex;
	private long ledgerId;
	private Class<? extends Exception> expectedException;
	private byte[] masterKey;
	private byte[] previousMasterKey;

	private boolean metadataExist;

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public SetMasterKeyTest(long ledgerId, byte[] masterKey, byte[] previousMasterKey, boolean metadataExist, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.masterKey = masterKey;
		this.previousMasterKey = previousMasterKey;
		this.metadataExist = metadataExist;
		this.expectedException = expectedException;
	}

	@Before
	public void configure() throws IOException {
		LedgerMetadataIndexConfig instance = new LedgerMetadataIndexConfig(metadataExist);

		// Vengono inseriti dei metadati nel ledger con id ledgerdId
		Map<byte[], byte[]> ledgers = instance.getLedgers();
		ByteBuffer buff = ByteBuffer.allocate(Long.BYTES);
		buff.putLong(ledgerId);

		// Testiamo il caso in cui esistono già dei metadati per il ledger con id
		// ledgerId nell'indice
		if (metadataExist) {
			LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false)
					.setMasterKey(ByteString.copyFrom(previousMasterKey)).build();
			ledgers.put(buff.array(), ledgerData.toByteArray());
			instance.setLedgers(ledgers);
		}

		ledgerMetadataIndex = instance.setupLedgerMetadaIndex();
	}

	@Parameterized.Parameters
	public static Collection<?> getTestParameters() {

		// ledgerId, masterKey, previousMasterKey, metadaExist, expectedException
		return Arrays.asList(new Object[][] { 
			
			{0, "testMasterKey".getBytes(), null,false, null},
			{1L, null, null, false, NullPointerException.class},
			
			// Aggiunti per migliorare coverage
			{0, "".getBytes(), new byte[0], true, null},												// 184
			{1L, new byte[0], "previousMasterKey".getBytes(), true, null},								// 190
			{1L, "testMasterKey".getBytes(), "previousMasterKey".getBytes(), true, IOException.class},	// stored!=master		
			{-1L, "testMasterKey".getBytes(), "testMasterKey".getBytes(), true, null},					// stored=master
		});
	}

	@Test
	public void setMasterKeyTest() throws IOException {
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		ledgerMetadataIndex.setMasterKey(ledgerId, masterKey);
		ledgerMetadataIndex.flush(); 
		
		LedgerData actualData = ledgerMetadataIndex.get(ledgerId);

		String newMasterKey = Arrays.toString(this.masterKey);
		String previousStoredMasterKey = Arrays.toString(this.previousMasterKey);
		
		System.out.println("newMasterKey: " + newMasterKey);
		System.out.println("actualMasterKey: " + Arrays.toString(actualData.getMasterKey().toByteArray()));
		
		String expectedMasterKey = null;
		if (this.previousMasterKey == null || this.previousMasterKey.length == 0) {					
            expectedMasterKey = newMasterKey;
        } else {
            expectedMasterKey = previousStoredMasterKey;
        }
		
		assertEquals(expectedMasterKey, Arrays.toString(actualData.getMasterKey().toByteArray()));

	}

}
