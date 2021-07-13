package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.protobuf.ByteString;

@RunWith(value = Parameterized.class)
public class SetLedgerMetadataTest {

	private LedgerMetadataIndex ledgerMetadataIndex;
	private LedgerMetadataIndexConfig instance;

	private long ledgerId;
	private LedgerData ledgerData;
	private Class<? extends Exception> expectedException;
	private boolean metadataAlreadyStored;

	@Rule
	public ExpectedException expectedRule = ExpectedException.none();

	public SetLedgerMetadataTest(long ledgerId, LedgerData ledgerData, Class<? extends Exception> expectedException,
			boolean metadataAlreadyStored) {
		this.ledgerId = ledgerId;
		this.ledgerData = ledgerData;
		this.expectedException = expectedException;
		this.metadataAlreadyStored = metadataAlreadyStored;
	}

	@Before
	public void configure() throws IOException {
		instance = new LedgerMetadataIndexConfig(false);
		ledgerMetadataIndex = instance.setupLedgerMetadaIndex();
	}

	@Parameterized.Parameters
	public static Collection<?> getTestParameters() {
		LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false)
				.setMasterKey(ByteString.copyFromUtf8("masterKeyTest")).build();
		boolean metadataAlreadyStored = true;
		
		return Arrays.asList(new Object[][] {
			
// 				ledgerId, ledgerData, expectedException, metadataAlreadyStored
				{ 0, ledgerData, null, metadataAlreadyStored }, 
				{ -1, null, NullPointerException.class, !metadataAlreadyStored },
				{ 1, ledgerData, null, metadataAlreadyStored } });
	}

	@Test
	public void test() throws IOException {

		if (expectedException != null) {
			expectedRule.expect(expectedException);
		}

		ledgerMetadataIndex.set(ledgerId, ledgerData);
		ledgerMetadataIndex.flush();
		LedgerData storedData = ledgerMetadataIndex.get(ledgerId);

		System.out.println("StoredData: " + storedData);
		System.out.println("LedgerData: " + ledgerData);

		if (metadataAlreadyStored) {
			ledgerMetadataIndex.set(ledgerId, storedData);				// Reinserimento ledger
		}
		assertEquals(ledgerData, storedData);

	}

}
