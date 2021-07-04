package org.apache.bookkeeper.bookie.storage.ldb;

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
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class DeleteLedgerTest extends LedgerMetadataIndexConfig{
	
	private long ledgerId;
	private LedgerMetadataIndex ledgerMetadataIndex;
	private boolean metadataExist;
	private boolean modifyData;

	private LedgerData ledgerData;
	private ByteBuffer buff;

	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public DeleteLedgerTest(long ledgerId, boolean metadataExist, boolean modifyData) {
		this.ledgerId = ledgerId;
		this.metadataExist = metadataExist;
		this.modifyData = modifyData;
	}

	@Before
	public void configure() throws IOException {
		LedgerMetadataIndexConfig instance = new LedgerMetadataIndexConfig(metadataExist);

		if (metadataExist) {
			Map<byte[], byte[]> ledgers = instance.getLedgers();
			buff = ByteBuffer.allocate(Long.BYTES);
			buff.putLong(ledgerId);
			this.ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false)
					.setMasterKey(ByteString.copyFromUtf8("testMasterKey")).build();
			ledgers.put(buff.array(), ledgerData.toByteArray());
			instance.setLedgers(ledgers);
		}

		ledgerMetadataIndex = instance.setupLedgerMetadaIndex();
	}

	@Parameterized.Parameters
	public static Collection<?> getTestParameters() {

		return Arrays.asList(new Object[][] {

				{ 0, false, false }, { -1, true, false }, { 1, true, true }, });
	}

	// In tutti i test deve essere sollevata questa eccezione.
	// Essa viene sollevata dal metodo get. Se ciò avviene significa che il ledger è stato eliminato correttamente
	@Test
	public void deleteTest() throws IOException{
		exceptionRule.expect(Bookie.NoLedgerException.class);
		
		if (modifyData) {
			LedgerData ledgerModifyData = LedgerData.newBuilder().setExists(true).setFenced(false)
					.setMasterKey(ByteString.copyFromUtf8("modifyMasterKey")).build();
			ledgerMetadataIndex.set(this.ledgerId, ledgerModifyData);
			// per coprire la seconda branch (!=) di pendingLedgersUpdates.removeIf(e -> e.getKey() == ledgerId);
			// Itera su tutte le entry dell'indice. Se trova altre entry riferite ad lederId le elimina.
			ledgerMetadataIndex.set(3L, ledgerModifyData); 
		}

		ledgerMetadataIndex.delete(this.ledgerId);
		ledgerMetadataIndex.flush();
		ledgerMetadataIndex.removeDeletedLedgers();
		ledgerMetadataIndex.get(this.ledgerId);						// Deve lanciare NoledgerException se l'eliminazione del ledger è avvenuta correttamente

	}

}
