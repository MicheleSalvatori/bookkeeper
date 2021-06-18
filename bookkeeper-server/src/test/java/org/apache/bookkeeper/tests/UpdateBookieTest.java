package org.apache.bookkeeper.tests;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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

@RunWith(value = Parameterized.class)
public class UpdateBookieTest {
	
	private static DefaultEnsemblePlacementPolicy policy;
	Map<BookieId, BookieInfo> bookieInfoMap;
	private Class<? extends Exception> expectedException;
	
	public UpdateBookieTest(Map<BookieId, BookieInfo> bookieInfoMap, Class<? extends Exception> expectedException ) {
		this.bookieInfoMap = bookieInfoMap;
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
		
		BookieId bookieId = new BookieSocketAddress("127.0.0.1", 5000).toBookieId();
		BookieInfo bookieInfo = new BookieInfo();
		
		HashMap<BookieId, BookieInfo> map = new HashMap<>();
		map.put(bookieId, bookieInfo);
		
	      return Arrays.asList(new Object[][] {
	    	  { new HashMap<>(), null}, 
	    	  { map, null},
	    	  { null, NullPointerException.class}
	      });
	}
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();
	
	@Test
	public void test(){
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		policy.updateBookieInfo(bookieInfoMap);
        
	}
	
	

//	private static HashSet<BookieId> getBookieIds(int quantity) {
//
//		HashSet<BookieId> output = new HashSet<>();
//		for (int i = 0; i < quantity; i++, nextBookieSocketAddressID++) {
//
//			String hostname = String.format("127.0.0.%d", nextBookieSocketAddressID);
//			int port = 3000 + nextBookieSocketAddressID;
//			BookieSocketAddress bookieSocketAddress = new BookieSocketAddress(hostname, port);
//
//			output.add(bookieSocketAddress.toBookieId());
//		}
//
//		return output;
//	}
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
