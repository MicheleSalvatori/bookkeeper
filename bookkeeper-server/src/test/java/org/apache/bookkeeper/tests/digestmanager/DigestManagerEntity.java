package org.apache.bookkeeper.tests.digestmanager;

import java.security.GeneralSecurityException;

import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.util.ByteBufList;

import io.netty.buffer.ByteBuf;

public class DigestManagerEntity {
	
	private int length;
	private int lacID, entryId, entryIdToTest; 
	private DigestType digestType;
	private DigestType digestTypeToTest;
	private int ledgerID, ledgerIdToTest;
	private boolean badByteBufList;
	private ByteBuf testBuf;
	private ByteBufList testBufList;
	private boolean isNull;
	
	private byte[] pass;
	private boolean useV2Protocol;
	
	public DigestManagerEntity(byte[] bs) {
		this.pass = bs;
	}
	
	public DigestManagerEntity(int lacID, DigestType digestType, int ledgerID, boolean useV2Protocol) throws GeneralSecurityException {
		this.lacID = lacID;
		this.digestType = digestType;
		this.ledgerID = ledgerID;
		this.useV2Protocol = useV2Protocol;
	}
	
	
	public DigestManagerEntity( int ledgerID, int entryId, DigestType digestType, DigestType digestTypeToTest, int ledgerIdToTest,
			int entryIdToTest, boolean badByteBufList, int length, boolean isNull) throws GeneralSecurityException {
		this.entryId = entryId;
		this.entryIdToTest = entryIdToTest;
		this.ledgerIdToTest = ledgerIdToTest;
		this.digestType = digestType;
		this.setDigestTypeToTest(digestTypeToTest);
		this.ledgerID = ledgerID;
		this.badByteBufList = badByteBufList;
		this.length = length;
		this.isNull = isNull;
	}
	
	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public boolean isBadByteBufList() {
		return badByteBufList;
	}

	public void setBadByteBufList(boolean badByteBufList) {
		this.badByteBufList = badByteBufList;
	}

	public boolean isNull() {
		return isNull;
	}

	public void setNull(boolean isNull) {
		this.isNull = isNull;
	}

	public int getLacID() {
		return lacID;
	}

	public void setLacID(int lacID) {
		this.lacID = lacID;
	}

	public DigestType getDigestType() {
		return digestType;
	}

	public void setDigestType(DigestType digestType) {
		this.digestType = digestType;
	}

	public long getLedgerID() {
		return ledgerID;
	}

	public void setLedgerID(int ledgerID) {
		this.ledgerID = ledgerID;
	}

	public boolean isUseV2Protocol() {
		return useV2Protocol;
	}

	public void setUseV2Protocol(boolean useV2Protocol) {
		this.useV2Protocol = useV2Protocol;
	}

	public ByteBuf getTestBuf() {
		return testBuf;
	}

	public void setTestBuf(ByteBuf testBuf) {
		this.testBuf = testBuf;
	}

	public byte[] getPass() {
		return pass;
	}

	public void setPass(byte[] pass) {
		this.pass = pass;
	}

	public int getEntryId() {
		return entryId;
	}

	public void setEntryId(int entryId) {
		this.entryId = entryId;
	}

	public ByteBufList getTestBufList() {
		return testBufList;
	}

	public void setTestBufList(ByteBufList testBufList) {
		this.testBufList = testBufList;
	}

	public DigestType getDigestTypeToTest() {
		return digestTypeToTest;
	}

	public void setDigestTypeToTest(DigestType digestTypeToTest) {
		this.digestTypeToTest = digestTypeToTest;
	}

	public int getEntryIdToTest() {
		return entryIdToTest;
	}

	public void setEntryIdToTest(int entryIdToTest) {
		this.entryIdToTest = entryIdToTest;
	}

	public int getLedgerIdToTest() {
		return ledgerIdToTest;
	}

	public void setLedgerIdToTest(int ledgerIdToTest) {
		this.ledgerIdToTest = ledgerIdToTest;
	}

}