package org.apache.bookkeeper.bookie.storage.ldb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.mockito.Mock;

public class LedgerMetadataIndexConfig {

	@SuppressWarnings("unchecked")
	@Mock
	private KeyValueStorage.CloseableIterator<Map.Entry<byte[], byte[]>> closeableIterator = 		// Iterator interface
			mock(KeyValueStorage.CloseableIterator.class); 							

	@Mock
	private KeyValueStorageFactory factory = mock(KeyValueStorageFactory.class); 					// Factory class
	@Mock
	private KeyValueStorage keyValueStorage = mock(KeyValueStorage.class); 

	private Iterator<Map.Entry<byte[], byte[]>> iterator;
	private Map<byte[], byte[]> ledgers; 						// lista di ledgers su cui itera l'iterator
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
		/*
		 * Nel caso in cui si simula la presenza di metadati già esisteni, 
		 * l'iterator ritorna il prossimo metadato
		 */

		if (this.metadataExist) {
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
