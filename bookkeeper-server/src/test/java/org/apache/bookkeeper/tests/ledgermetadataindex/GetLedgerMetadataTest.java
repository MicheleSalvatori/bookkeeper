package org.apache.bookkeeper.tests.ledgermetadataindex;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.protobuf.ByteString;

@RunWith(value = Parameterized.class)
public class GetLedgerMetadataTest {

	private long ledgerId;
	private LedgerMetadataIndex ledgerMetadataIndex;
	private LedgerData ledgerData;
	Class<? extends Exception> expectedException;

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public GetLedgerMetadataTest(long ledgerId, LedgerData ledgerData, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.ledgerData = ledgerData;
		this.expectedException = expectedException;
	}

	@Before
	public void configure() throws IOException {
		LedgerMetadataIndexConfig instance = new LedgerMetadataIndexConfig(true);

		Map<byte[], byte[]> ledgers = instance.getLedgers();
		ByteBuffer buff = ByteBuffer.allocate(Long.BYTES);
		buff.putLong(ledgerId);
		
		if (ledgerData != null) {
			ledgers.put(buff.array(), ledgerData.toByteArray());
		}
		
		instance.setLedgers(ledgers);
		ledgerMetadataIndex = instance.setupLedgerMetadaIndex();
	}

	@Parameterized.Parameters
	public static Collection<?> getTestParameters() {
		LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.EMPTY)
				.build();

		return Arrays.asList(new Object[][] {

				{ 0, ledgerData, null },
				{ -1, null, Bookie.NoLedgerException.class }, });
	}

	@Test
	public void testGet() throws IOException {													// Ragionare se mettere anche il caso in cui il ledger non esista nel db
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		LedgerData actualData = ledgerMetadataIndex.get(ledgerId);
		System.out.println(actualData);

		assertEquals(this.ledgerData, actualData);
	}

}
