package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieIdTest;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

@RunWith(value = Parameterized.class)
public class DefaultEnsemblePlacementPolicyTest {
	
	private static int DEFAULT_BOOKIES_KNOWN = 10;
	private static int DEFAULT_BOOKIES_EXCLUDED = 5;

	private static DefaultEnsemblePlacementPolicy policy;
	private static int nextBookieSocketAddressID;
	private static Set<BookieId> writableBookies;
	private static Set<BookieId> deadBookies;

	private int ensembleSize;
	private int quorumSize;
	private int ackQuorumSize;
	private Map<String, byte[]> customMetadata;
	private static Set<BookieId> excludeBookies;
	private Class<? extends Exception> expectedException;
	private int expectedValue;

	// newEnsemble(int ensembleSize, int quorumSize, int ackQuorumSize, Map<String,
	// byte[]> customMetadata, Set<BookieId> excludeBookies)

	public DefaultEnsemblePlacementPolicyTest(int ensembleSize, int quorumSize, int ackQuorumSize,
			Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies, int expectedValue, Class<? extends Exception> expectedException) {
		this.quorumSize = quorumSize;
		this.ackQuorumSize = ackQuorumSize;
		this.ensembleSize = ensembleSize;
		this.customMetadata = customMetadata;
		this.excludeBookies = excludeBookies;
		this.expectedValue = expectedValue;
		this.expectedException = expectedException;

	}

	@BeforeClass
	public static void configure() {
		policy = new DefaultEnsemblePlacementPolicy();

		// Si poteva anche non utilizzare una mock ma avrei dovuto configurare un client
		// locale

		ClientConfiguration conf = Mockito.mock(ClientConfiguration.class);
		Mockito.when(conf.getDiskWeightBasedPlacementEnabled()).thenReturn(true);
		Mockito.when(conf.getBookieMaxWeightMultipleForWeightBasedPlacement()).thenReturn(5);

		policy.initialize(conf, Optional.empty(), null, null, null, null);
		
		deadBookies = policy.onClusterChanged(writableBookies, new HashSet<>());			// deve essere chiamata per inizializzare la variabile knowBookies della policy
	}

	@Parameters
	public static Collection<Object[]> getParameters() {
		System.out.println("Parameters");
		writableBookies = getBookieIds(DEFAULT_BOOKIES_KNOWN);									// Vengono instanziati i bookie che il server di Bookkeeper conosce		TODO spostare in configure e metterlo beforeClass
		excludeBookies  = getBookieSocketAddressesFrom(writableBookies, DEFAULT_BOOKIES_EXCLUDED);  // generiamo una lista di bookie da escludere dal nuovo ensemble
		
		return Arrays.asList(new Object[][] { 												// HashSet implements Set
				 
			// Test suit minimale
				{ 0, 0, 0, null, new HashSet<>(), 0, null}
				,{2,1,0,null, excludeBookies, 2, null}
				,{2,3,4,null, null, 0, NullPointerException.class}											// Tutta via non vengono controllati gli interi nel metodo under test
				,{-1,-1,-1,null, null, 0, IllegalArgumentException.class}						
				});
	}
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@Test
	public void test() throws BKNotEnoughBookiesException {
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		
		assertEquals(0, deadBookies.size());									// Ci assicuriamo che non siano stati creati bookie non attivi
		
		System.out.println("WritableBookies: "+writableBookies);
		System.out.println("DeadBookies: "+deadBookies);
		System.out.println("ExcludeBookies: "+excludeBookies);
		
		List<BookieId> ensembleCreated = policy.newEnsemble(ensembleSize, quorumSize, ackQuorumSize, customMetadata, excludeBookies).getResult();				// Viene creato un nuovo ensemble dai bookie che il nostro server di bookkeeper conosce
																													// Da essi vengono esclusi dall'insieme gli excludeBookies generati prima
		System.out.println("EnsebleCreated: "+ensembleCreated);
		assertEquals(expectedValue, ensembleCreated.size());
		
		// Assert that new enseble doen't contain any excludeBookies
		 for (BookieId outputBookie : ensembleCreated) {
             assertFalse(excludeBookies.contains(outputBookie));
		 }
	}
	
	

	private static HashSet<BookieId> getBookieIds(int quantity) {

		HashSet<BookieId> output = new HashSet<>();
		for (int i = 0; i < quantity; i++, nextBookieSocketAddressID++) {

			String hostname = String.format("127.0.0.%d", nextBookieSocketAddressID);
			int port = 3000 + nextBookieSocketAddressID;
			BookieSocketAddress bookieSocketAddress = new BookieSocketAddress(hostname, port);

			output.add(bookieSocketAddress.toBookieId());
		}

		return output;
	}

	private static Set<BookieId> getBookieSocketAddressesFrom(Set<BookieId> source, int quantity) {
		Set<BookieId> output = new HashSet<>();

		int i = 0;

		for (BookieId x : source) {

			output.add(x);
			i++;

			if (i == quantity)
				break;
		}

		return output;
	}

}
