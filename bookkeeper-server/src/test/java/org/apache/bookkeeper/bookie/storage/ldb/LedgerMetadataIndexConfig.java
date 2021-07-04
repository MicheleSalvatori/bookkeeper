package org.apache.bookkeeper.bookie.storage.ldb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory;
import org.apache.bookkeeper.bookie.storage.ldb.LedgerMetadataIndex;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.CachingStatsLogger;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.mockito.Mock;

public class LedgerMetadataIndexConfig {

	@SuppressWarnings("unchecked")
	@Mock
	private KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator = mock(
			KeyValueStorage.CloseableIterator.class); 							// Iterator interface

	@Mock
	private KeyValueStorageFactory factory = mock(KeyValueStorageFactory.class); // Factory class to create
																						// instances of the key-value
																						// storage implementation

	@Mock
	private KeyValueStorage keyValueStorage = mock(KeyValueStorage.class); // Abstraction of a generic key-value
																					// local database.

	private Iterator<Map.Entry<byte[], byte[]>> iterator;
	private Map<byte[], byte[]> ledgers; 						// è una lista di tutti i ledgers su cui itera l'iterator
	private boolean metadataExist;
	
	public LedgerMetadataIndexConfig() {}

	public LedgerMetadataIndexConfig(boolean metadataExist) {
		this.ledgers = new HashMap<byte[], byte[]>();
		this.metadataExist = metadataExist;
	}

	public LedgerMetadataIndex setupLedgerMetadaIndex() throws IOException {

		// mock setup
		when(factory.newKeyValueStorage(any(), any(), any(), any())).thenReturn(keyValueStorage);
		when(keyValueStorage.iterator()).then(invocationOnMock -> {
			iterator = ledgers.entrySet().iterator();
			return closeableIterator;
		});

		if (this.metadataExist) { // ritorna il prossimo dall'iterator se stiamo simulando la presenza già dei
									// metadati nell'indice
			when(closeableIterator.hasNext()).then(invocationOnMock -> this.iterator.hasNext());
			when(closeableIterator.next()).then(invocationOnMock -> this.iterator.next());
		} else {
			when(closeableIterator.hasNext()).thenReturn(false);
		}
		return new LedgerMetadataIndex(new ServerConfiguration(), factory, "testPath", new NullStatsLogger());
	}

	public Map<byte[], byte[]> getLedgers() {
		return ledgers;
	}

	public void setLedgers(Map<byte[], byte[]> ledgers) {
		this.ledgers = ledgers;
	}

	public KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> getCloseableIterator() {
		return closeableIterator;
	}


	public  KeyValueStorageFactory getFactory() {
		return factory;
	}

	public KeyValueStorage getKeyValueStorage() {
		return keyValueStorage;
	}

	public Iterator<Map.Entry<byte[], byte[]>> getIterator() {
		return iterator;
	}

	public void setIterator(Iterator<Map.Entry<byte[], byte[]>> iterator) {
		this.iterator = iterator;
	}

	public boolean isMetadataExist() {
		return metadataExist;
	}

	public void setMetadataExist(boolean metadataExist) {
		this.metadataExist = metadataExist;
	}

}
