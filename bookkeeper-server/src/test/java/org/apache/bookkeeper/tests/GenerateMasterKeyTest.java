package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.MacDigestManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


//public ByteBuf internalReadEntry(long ledgerId, long entryId, long location, boolean validateEntry)

@RunWith(value = Parameterized.class)
public class GenerateMasterKeyTest{
	
	private static final String EMPTY_LEDGER_KEY_PAD = "ledger";					
	private static final byte[] EMPTY_LEDGER_KEY_PASSWD = new byte[0];
	
	
	private byte[] pass;
	private byte[] expectedResult;
	private static int byteLenght = 50;
	private static byte[] randomByte = null;
	
	public GenerateMasterKeyTest(byte[] pass, byte[] expectedResult) {					// Fare poi una DigestManagerEntity e mettere tutto li
		this.pass = pass;
		this.expectedResult = expectedResult;
	}
	
	@BeforeClass
	public static void configure(){
		System.out.println("configure");
	}
	
	@Parameters
	public static Collection<Object[]> data() throws NoSuchAlgorithmException {
		randomByte = new byte[byteLenght];
		
		return Arrays.asList(new Object[][] { 
			{"".getBytes(), MacDigestManager.genDigest(EMPTY_LEDGER_KEY_PAD, EMPTY_LEDGER_KEY_PASSWD) }
			,{randomByte, MacDigestManager.genDigest("ledger", randomByte)}
		});
	}
	
	
	@Test
	public void test() throws NoSuchAlgorithmException {
		byte[] result = DigestManager.generateMasterKey(pass);
		System.out.println(new String(result));
		System.out.println(new String(expectedResult));
		assertArrayEquals((byte[]) expectedResult, result);
	}

}
