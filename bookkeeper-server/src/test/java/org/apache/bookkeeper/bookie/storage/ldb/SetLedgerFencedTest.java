package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class SetLedgerFencedTest {
	
	private LedgerMetadataIndexConfig instance;
	private LedgerMetadataIndex ledgerMetadataIndex;

	private long ledgerId;
	private Class<? extends Exception> expectedException;
	private boolean simulateConcurrency;
	private boolean alreadyFenced;
	private boolean ledgerExist;

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public SetLedgerFencedTest(long ledgerId, boolean simulateConcurrency, boolean alreadyFenced, boolean ledgerExist, Class<? extends Exception> expectedException) {
		this.ledgerId = ledgerId;
		this.simulateConcurrency = simulateConcurrency;
		this.alreadyFenced = alreadyFenced;
		this.ledgerExist = ledgerExist;
		this.expectedException = expectedException;
	}

	@Before
	public void configure() throws IOException {
		instance = new LedgerMetadataIndexConfig(ledgerExist);
		LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(alreadyFenced)					// alreadyFenced = true -> il ledger avrà il fencing già abilitato
				.setMasterKey(ByteString.copyFromUtf8("masterKeyTest"+ledgerId)).build();
		
		if (ledgerExist) {
			Map<byte[], byte[]> ledgers = instance.getLedgers();
			ByteBuffer buff = ByteBuffer.allocate(Long.BYTES);
			buff.putLong(ledgerId);
			ledgers.put(buff.array(), ledgerData.toByteArray());
			instance.setLedgers(ledgers);
		}
		
		ledgerMetadataIndex = instance.setupLedgerMetadaIndex();
		
		if (simulateConcurrency) {
			ledgerMetadataIndex = spy(ledgerMetadataIndex);
			when(ledgerMetadataIndex.get(ledgerId)).then(invocation ->{
				LedgerData ledgerDataConcurr = (LedgerData) invocation.callRealMethod();
				ledgerMetadataIndex.delete(ledgerId);
				return ledgerDataConcurr;
			});
		}
	}

	@Parameterized.Parameters
	public static Collection<?> getTestParameters() {
//		ledgerId, simulateConcurrency, alreadyFenced, ledgerExist
		return Arrays.asList(new Object[][] {						// mettere dei parametri booleani qui in modo da capire cosa sono dal nome

				{ 0, false, false, false, Bookie.NoLedgerException.class }, 	// test ledger non esiste
				{-1, false, true, true, null }, 								// test ledger già fenced
				{0, false, false, true, null }, 								// test ledger non fenced
				{1, true, false, true, null }, 									// simulate concurrency
				});
	}

	@Test
	public void testSetFenced() throws IOException { 
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		
		boolean returnValue = ledgerMetadataIndex.setFenced(this.ledgerId);		// true = fencing abilitato, false = fencing già abilitato
		ledgerMetadataIndex.flush();
		
		if (alreadyFenced)
			assertFalse(returnValue);
		else assertTrue(returnValue);
		
		LedgerData updatedData = ledgerMetadataIndex.get(this.ledgerId);		// recuperiamo il ledger che abbiamo modificato
		assertTrue(updatedData.getFenced());									// ci assicuriamo che il fencing sia abilitato
		
		if (returnValue) {														// Check sulla corretta invocazione del metodo put sulla nostra mock
			ByteBuffer buff = ByteBuffer.allocate(Long.BYTES);
			buff.putLong(ledgerId);
			LedgerData expectedLedgerData = LedgerData.newBuilder().setExists(true).setFenced(true).setMasterKey(ByteString.copyFromUtf8("masterKeyTest"+ledgerId)).build();
			verify(instance.getKeyValueStorage()).put(buff.array(), expectedLedgerData.toByteArray());
		}
	}

}
