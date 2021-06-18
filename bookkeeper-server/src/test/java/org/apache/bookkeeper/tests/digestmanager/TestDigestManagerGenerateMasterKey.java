package org.apache.bookkeeper.tests.digestmanager;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.proto.checksum.MacDigestManager;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;




@RunWith(Parameterized.class)
public class TestDigestManagerGenerateMasterKey {
	

	private byte[] pass;

	private Object expectedResult;
	private static int stringLenght = 50;
	static byte[] b = null;
	
	public static void generatesBytes() {
		b = new byte[stringLenght];
		Random rnd = new Random();
		rnd.nextBytes(b);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> DigestManagerGenerateMasterKeyParameters() throws Exception {
		generatesBytes();
		
		return Arrays.asList(new Object[][] {
			
			// Suite minimale
			{new DigestManagerEntity("".getBytes()), MacDigestManager.EMPTY_LEDGER_KEY},
			{new DigestManagerEntity(b), MacDigestManager.genDigest("ledger", b)}
		});
	}
	
	public TestDigestManagerGenerateMasterKey(DigestManagerEntity entity, Object expectedResult){
		if (entity!=null)
			this.pass = entity.getPass();
		this.expectedResult = expectedResult;
	}

	@Test
	public void testGenerateMasterKey() {
		try {
			Assert.assertArrayEquals((byte[]) expectedResult, DigestManager.generateMasterKey(pass));
		} catch (Exception e) {
		}
	}
}  