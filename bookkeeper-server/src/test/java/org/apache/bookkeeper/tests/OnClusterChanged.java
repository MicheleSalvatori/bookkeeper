package org.apache.bookkeeper.tests;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.bookkeeper.client.BookieInfoReader.BookieInfo;
import org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;
import org.mockito.internal.verification.Only;

@RunWith(value = Parameterized.class)
public class OnClusterChanged{
	
	private static DefaultEnsemblePlacementPolicy policy;
	private Set<BookieId> writableBookies;
	private Set<BookieId> readOnlyBookies;
	private int expectedValue;
	private Class<? extends Exception> expectedException;
	
	private static int nextBookieSocketAddressID;
	
	public OnClusterChanged(Set<BookieId> writableBookies, Set<BookieId> readOnlyBookies, int expectedValue, Class<? extends Exception> expectedException ) {
		this.writableBookies = writableBookies;
		this.readOnlyBookies = readOnlyBookies;
		this.expectedValue = expectedValue;
		this.expectedException = expectedException;
	}
	
	@BeforeClass
	public static void configureEnvironment() {
		System.out.println("ConfigureEnvironment");
		policy = new DefaultEnsemblePlacementPolicy();
		
		ClientConfiguration conf = Mockito.mock(ClientConfiguration.class);
		Mockito.when(conf.getDiskWeightBasedPlacementEnabled()).thenReturn(true);
		Mockito.when(conf.getBookieMaxWeightMultipleForWeightBasedPlacement()).thenReturn(5);
		
		policy.initialize(conf, Optional.empty(), null, null, null, null);
	}
	
	@Parameters
	public static Collection<Object[]> primeNumbers() {
		
		HashSet<BookieId> writableBookies = getBookieIds(1);
		HashSet<BookieId> readOnlyBookies = getBookieIds(1);
		HashSet<BookieId> wrongReadOnlyBookies = getBookieIdsBad(1);
		HashSet<BookieId> wrongWritableBookies = writableBookies;
		wrongWritableBookies.add(readOnlyBookies.iterator().next());
		
		
	      return Arrays.asList(new Object[][] {
	    	  { writableBookies, readOnlyBookies, 0, null}, 
	    	  { wrongWritableBookies, wrongReadOnlyBookies, 0, null},
	    	  { null, null, 0, NullPointerException.class},
	      });
	}
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@Test
	public void test(){
		System.out.println("Writable: "+writableBookies);
		System.out.println("ReadOnly: "+readOnlyBookies);
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		
		System.out.println(policy.onClusterChanged(writableBookies, readOnlyBookies));
        
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
	
	private static HashSet<BookieId> getBookieIdsBad(int quantity) {

		HashSet<BookieId> output = new HashSet<>();
		for (int i = 0; i < quantity; i++, nextBookieSocketAddressID++) {

			String hostname = String.format("194.0.0.%d", nextBookieSocketAddressID);
			int port = 3000 + nextBookieSocketAddressID;
			BookieSocketAddress bookieSocketAddress = new BookieSocketAddress(hostname, port);

			output.add(bookieSocketAddress.toBookieId());
		}

		return output;
	}
//
//	private static Set<BookieId> getBookieSocketAddressesFrom(Set<BookieId> source, int quantity) {
//		Set<BookieId> output = new HashSet<>();
//
//		int i = 0;
//
//		for (BookieId x : source) {
//
//			output.add(x);
//			i++;
//
//			if (i == quantity)
//				break;
//		}
//
//		return output;
//	}

}
