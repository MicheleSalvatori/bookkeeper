package org.apache.bookkeeper.tests;

import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.net.BookieId;

public class TestParameters {
	
	private int ensembleSize;
	private int quorumSize;
	private int ackQuorumSize;
	private Map<String, byte[]> customMetadata;
	private Set<BookieId> excludeBookies;
	private int expectedValue;
	private Class<? extends Exception> expectedException;
	
	public TestParameters(int ensembleSize, int quorumSize, int ackQuorumSize, Map<String, byte[]> customMetadata, Set<BookieId> excludeBookies, int expectedValue, Class<? extends Exception> expectedException) {
		this.ensembleSize = ensembleSize;
		this.quorumSize = quorumSize;
		this.ackQuorumSize = ackQuorumSize;
		this.customMetadata = customMetadata;
		this.excludeBookies = excludeBookies;
		this.expectedValue = expectedValue;
		this.expectedException = expectedException;
	}

	public int getEnsembleSize() {
		return ensembleSize;
	}

	public void setEnsembleSize(int ensembleSize) {
		this.ensembleSize = ensembleSize;
	}

	public int getQuorumSize() {
		return quorumSize;
	}

	public void setQuorumSize(int quorumSize) {
		this.quorumSize = quorumSize;
	}

	public int getAckQuorumSize() {
		return ackQuorumSize;
	}

	public void setAckQuorumSize(int ackQuorumSize) {
		this.ackQuorumSize = ackQuorumSize;
	}

	public Map<String, byte[]> getCustomMetadata() {
		return customMetadata;
	}

	public void setCustomMetadata(Map<String, byte[]> customMetadata) {
		this.customMetadata = customMetadata;
	}

	public Set<BookieId> getExcludeBookies() {
		return excludeBookies;
	}

	public void setExcludeBookies(Set<BookieId> excludeBookies) {
		this.excludeBookies = excludeBookies;
	}

	public int getExpectedValue() {
		return expectedValue;
	}

	public void setExpectedValue(int expectedValue) {
		this.expectedValue = expectedValue;
	}

	public Class<? extends Exception> getExpectedException() {
		return expectedException;
	}

	public void setExpectedException(Class<? extends Exception> expectedException) {
		this.expectedException = expectedException;
	}
	
	
}
