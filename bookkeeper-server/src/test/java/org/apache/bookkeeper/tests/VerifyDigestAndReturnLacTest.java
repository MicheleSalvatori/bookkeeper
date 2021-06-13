package org.apache.bookkeeper.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.listeners.InvocationListener;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

// private void verifyDigest(long entryId, ByteBuf dataReceived, boolean skipEntryIdCheck)

@RunWith(value = Parameterized.class)
public class VerifyDigestAndReturnLacTest {

	private TestInput entityTestInput;
	private ByteBuf dataReceived;
	private Class<? extends Exception> expectedException;

	private static long ledgerId = 0;
	private static byte[] passwd = "testPassword".getBytes();

	private DigestManager digestReceiver;
	private DigestManager digestSender;

	private static ByteBuf validInstance;
	private ByteBufList byteBufList;

	public VerifyDigestAndReturnLacTest( ByteBuf dataReceived, Class<? extends Exception> expectedException) {
		this.entityTestInput = entityTestInput;
		this.dataReceived = dataReceived;
		this.expectedException = expectedException;
	}

	@Before
	public void configure() throws GeneralSecurityException {

		digestReceiver = DigestManager.instantiate(ledgerId, passwd, entityTestInput.getDigestType(),
				UnpooledByteBufAllocator.DEFAULT, false);
		digestSender = DigestManager.instantiate(ledgerId, passwd, entityTestInput.getDigestType(),
				UnpooledByteBufAllocator.DEFAULT, false);
		
		if (dataReceived != null) {
			byteBufList = digestSender.computeDigestAndPackageForSendingLac(1L);
		}

	}

	@Parameters
	public static Collection<Object[]> data() throws NoSuchAlgorithmException {
		validInstance = Unpooled.buffer(DigestManager.METADATA_LENGTH);
		validInstance.writeBytes(new byte[9]);
		
		return Arrays.asList(new Object[][] { 
			// Suit minimale
			 { validInstance, null }
		});
	}
	
	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	@Test
	public void test() {
		
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}

		try {
			long result = digestReceiver.verifyDigestAndReturnLac(byteBufList.getBuffer(0));

			System.out.println(result); // L'assert funziona
//			assertEquals(expectedData, result);
			
		} catch (BKDigestMatchException e) {
			e.printStackTrace();
		}
	}

}
