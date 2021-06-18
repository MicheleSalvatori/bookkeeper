package org.apache.bookkeeper.tests.digestmanager;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.tests.digestmanager.*;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class TestDigestManagerVerifyDigest {

	private Object expectedResult;
	
	private DigestManagerEntity entity;
	

	private DigestManager digestManager;
	private DigestManager digestForSending;
	private boolean checked = false;
	
    public void generatesByteBuf(int lenght) {
    	//la seconda chiamata viene ignorata in quanto il buffer è già stato creato
    	if (!checked) {
    		Random rnd = new Random();
        	byte[] data = new byte[lenght];
    		rnd.nextBytes(data);
    		
    		ByteBuf bb = Unpooled.buffer(DigestManager.METADATA_LENGTH);
    		bb.writeBytes(data);
    		this.entity.setTestBuf(bb);
    		this.checked = true;
		}
	}
    
   
	
	@Parameterized.Parameters
	public static Collection<Object[]> DigestManagerVerifyDigestParameters() throws Exception {
		return Arrays.asList(new Object[][] {
			
			// Suite minimale
			{new DigestManagerEntity(0, -1, DigestType.HMAC, DigestType.HMAC, 0, -1, false, 12, true), NullPointerException.class},
			{new DigestManagerEntity(0, 1, DigestType.HMAC, DigestType.HMAC, 0, 1, false, 12, false), 0},
			{new DigestManagerEntity(0, 0, DigestType.HMAC, DigestType.HMAC, 0, 0, false, 12, false), 0},

			// Coverage
			{new DigestManagerEntity(0, 1, DigestType.HMAC, DigestType.HMAC, 0, 0, false, 12, false), BKDigestMatchException.class},//entry
			{new DigestManagerEntity(1, 1, DigestType.DUMMY, DigestType.DUMMY, -1, 1, false, 12, false), BKDigestMatchException.class},//ledger
			{new DigestManagerEntity(1, 1, DigestType.HMAC, DigestType.CRC32, 1,1, false, 12, false), BKDigestMatchException.class},//mac
			{new DigestManagerEntity(1, 1, DigestType.HMAC, DigestType.CRC32C,1,1 ,true, 12, false), BKDigestMatchException.class},
			
			//mutation
			{new DigestManagerEntity(1, 1, DigestType.CRC32C, DigestType.CRC32,1,1 ,false, 0, false), BKDigestMatchException.class},
			{new DigestManagerEntity(0, 1, DigestType.HMAC, DigestType.HMAC, 0, 1, false, 0, false), 0},		
		});
	}

	public TestDigestManagerVerifyDigest(DigestManagerEntity entity, Object expectedResult){
		this.entity = entity;
		if (expectedResult.equals(0)) {
			generatesByteBuf(entity.getLength());
			this.expectedResult = entity.getTestBuf();
		}else {
			this.expectedResult = expectedResult;
		}
	}


	@Before
	public void beforeTest() throws GeneralSecurityException {
		this.digestManager = DigestManager.instantiate(entity.getLedgerIdToTest(), 
				"testPassword".getBytes(), entity.getDigestTypeToTest(), UnpooledByteBufAllocator.DEFAULT, false);
		this.digestForSending = DigestManager.instantiate(entity.getLedgerID(), 
				"testPassword".getBytes(), entity.getDigestType(), UnpooledByteBufAllocator.DEFAULT, false);
		generatesByteBuf(entity.getLength());
		ByteBuf bb = this.entity.getTestBuf();
		if (entity.isBadByteBufList()) {
			/*header  byte leggibili 0*/
		    ByteBuf badHeader = Unpooled.buffer(DigestManager.METADATA_LENGTH);
		    entity.setTestBufList(ByteBufList.get(badHeader, bb));
		    return;
		}
		if (entity.isNull()) {
			entity.setTestBufList(null);
			return;
		}
		ByteBufList byteBufList = digestForSending.computeDigestAndPackageForSending(entity.getEntryId(), 
				0, this.entity.getTestBuf().readableBytes(), bb);
		this.entity.setTestBufList(byteBufList);

	}
	
	@Test
	public void testVerifyDigestData() throws GeneralSecurityException{

		try {
			Assert.assertEquals(expectedResult, digestManager.verifyDigestAndReturnData(entity.getEntryIdToTest(), 
					ByteBufList.coalesce(entity.getTestBufList())));
		} catch (Exception e) {
			Assert.assertEquals(expectedResult, e.getClass());
		}
	}

}  

