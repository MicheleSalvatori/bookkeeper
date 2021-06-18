package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
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
public class ReplaceBookieTest {
	
	private static int DEFAULT_BOOKIES_KNOWN = 10;
	private static int DEFAULT_BOOKIES_EXCLUDED = 5;

	private static DefaultEnsemblePlacementPolicy policy;
	private static int nextBookieSocketAddressID;
	private static Set<BookieId> writableBookies;
	private static Set<BookieId> excludeBookies;
	private static List<BookieId> currentEnsemble;
	
	private static TestParameters parameters;
	private static int ensembleSize;
	private static int quorumSize;
	private static int ackQuorumSize;

	public ReplaceBookieTest(TestParameters parameters) {
		this.parameters = parameters;
		this.ensembleSize = parameters.getEnsembleSize();
		this.quorumSize = parameters.getQuorumSize();
		this.ackQuorumSize = parameters.getAckQuorumSize();

	}
	
	@BeforeClass
	public static void configureEnvironment() {
		System.out.println("ConfigureEnvironment");
		policy = new DefaultEnsemblePlacementPolicy();
		
		ClientConfiguration conf = Mockito.mock(ClientConfiguration.class);
		Mockito.when(conf.getDiskWeightBasedPlacementEnabled()).thenReturn(false);
		Mockito.when(conf.getBookieMaxWeightMultipleForWeightBasedPlacement()).thenReturn(5);
		
		policy.initialize(conf, Optional.empty(), null, null, null, null);
		
		writableBookies = getBookieIds(DEFAULT_BOOKIES_KNOWN);		
		System.out.println(writableBookies);
//		excludeBookies  = getBookieSocketAddressesFrom(writableBookies, DEFAULT_BOOKIES_EXCLUDED);  
		excludeBookies  = new HashSet<>(); 
		policy.onClusterChanged(writableBookies, new HashSet<>());			
	}
	
	@Parameters
	public static Collection<TestParameters> getParameters() throws BKNotEnoughBookiesException {
		System.out.println("Parameters");
		List<TestParameters> parameters = new ArrayList<>();
		parameters.add(new TestParameters(5, 5, 5, null, excludeBookies, 2, null));
		
		return parameters;
	}
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@Test
	public void test() throws BKNotEnoughBookiesException {
		
		currentEnsemble = policy.newEnsemble(ensembleSize, quorumSize, ackQuorumSize, parameters.getCustomMetadata(), excludeBookies).getResult();
		
		if (parameters.getExpectedException() != null) {
			exceptionRule.expect(parameters.getExpectedException());
		}
		
		BookieId bookieToReplace = currentEnsemble.get(4);		// mettere random
		BookieId output = policy.replaceBookie(ensembleSize, quorumSize, ackQuorumSize, parameters.getCustomMetadata(),  currentEnsemble, bookieToReplace, excludeBookies).getResult();
		
		assertNotEquals(bookieToReplace, output);
		
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
