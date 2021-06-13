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
public class DefaultEnsemblePlacementPolicyWithEntityTest {
	
	private static int DEFAULT_BOOKIES_KNOWN = 10;
	private static int DEFAULT_BOOKIES_EXCLUDED = 5;

	private static DefaultEnsemblePlacementPolicy policy;
	private static int nextBookieSocketAddressID;
	private static Set<BookieId> writableBookies;
	private static Set<BookieId> deadBookies;
	private static Set<BookieId> excludeBookies;
	
	private TestParameters parameters;

	// newEnsemble(int ensembleSize, int quorumSize, int ackQuorumSize, Map<String,
	// byte[]> customMetadata, Set<BookieId> excludeBookies)

	public DefaultEnsemblePlacementPolicyWithEntityTest(TestParameters parameters) {
		this.parameters = parameters;

	}

	public static void configure() {
		System.out.println("Configure");
		policy = new DefaultEnsemblePlacementPolicy();

		// Si poteva anche non utilizzare una mock ma avrei dovuto configurare un client
		// locale

		ClientConfiguration conf = Mockito.mock(ClientConfiguration.class);
		Mockito.when(conf.getDiskWeightBasedPlacementEnabled()).thenReturn(true);
		Mockito.when(conf.getBookieMaxWeightMultipleForWeightBasedPlacement()).thenReturn(5);

		policy.initialize(conf, Optional.empty(), null, null, null, null);
		
		writableBookies = getBookieIds(DEFAULT_BOOKIES_KNOWN);									// Vengono instanziati i bookie che il server di Bookkeeper conosce		TODO spostare in configure e metterlo beforeClass
		excludeBookies  = getBookieSocketAddressesFrom(writableBookies, DEFAULT_BOOKIES_EXCLUDED);  // generiamo una lista di bookie da escludere dal nuovo ensemble
		
		deadBookies= policy.onClusterChanged(writableBookies, new HashSet<>());			// deve essere chiamata per inizializzare la variabile knowBookies della policy
	}
	

	@Parameters
	public static Collection<TestParameters> getParameters() {
		System.out.println("Parameters");
		configure();
		List<TestParameters> parameters = new ArrayList<>();
		
		parameters.add(new TestParameters(0, 0, 0, null, new HashSet<>(), 0, null));
		parameters.add(new TestParameters(2, 1, 0, null, excludeBookies, 2, null));
		
		return parameters;
	}
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@Test
	public void test() throws BKNotEnoughBookiesException {
		
		if (parameters.getExpectedException() != null) {
			exceptionRule.expect(parameters.getExpectedException());
		}
		
		assertEquals(0, deadBookies.size());									// Ci assicuriamo che non siano stati creati bookie non attivi
		
		System.out.println("WritableBookies: "+writableBookies);
		System.out.println("DeadBookies: "+deadBookies);
		System.out.println("ExcludeBookies: "+excludeBookies);
		
		// Viene creato un nuovo ensemble dai bookie che il nostro server di bookkeeper conosce
		// Da essi vengono esclusi dall'insieme gli excludeBookies generati prima
		List<BookieId> ensembleCreated = policy.newEnsemble(parameters.getEnsembleSize(), parameters.getQuorumSize(), 
				parameters.getAckQuorumSize(), parameters.getCustomMetadata(), parameters.getExcludeBookies()).getResult();				
		
		System.out.println("EnsebleCreated: "+ensembleCreated);
		assertEquals(parameters.getExpectedValue(), ensembleCreated.size());
		
		// Assert that new enseble doen't contain any excludeBookies
		 for (BookieId outputBookie : ensembleCreated) {
             assertFalse(parameters.getExcludeBookies().contains(outputBookie));
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
