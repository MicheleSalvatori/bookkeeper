package org.apache.bookkeeper.bookie;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class FileInfoAbsoluteReadTest {

	private Class<? extends Exception> expectedException;
	private FileInfo fileInfo;
	private File lf;
	private FileChannel fc;
	
	/*
	 * Header signature
	 */
	private static final int SIGNATURE = ByteBuffer.wrap("BKLE".getBytes(UTF_8)).getInt();
	private static final long START_OF_DATA = 1024;
	private static final int BUFF_CAPACITY= 1024;
	private static byte[] masterKey = "masterKey".getBytes();
	private static int V0 = 0; 
	
	private int byteWrited;
	private static final int numOfBuffs = 5;
	
	private ByteBuffer buffer;
	private long start;
	private boolean bestEffort;
	private int bufferReaderCapacity;


	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public FileInfoAbsoluteReadTest(ByteBuffer buffer, long start, boolean bestEffort, Class<? extends Exception> expectedException, int bufferReaderCapacity) {
		this.buffer = buffer;
		this.start = start;
		this.bestEffort = bestEffort;
		this.expectedException = expectedException;
		this.bufferReaderCapacity = bufferReaderCapacity;
	}

	@Before
	public void setUp() throws Exception {
		this.lf = new File("ledgerIndexFile.txt");
		fc = new RandomAccessFile(lf, "rws").getChannel();
		
		// Header
		ByteBuffer bb = ByteBuffer.allocate((int) START_OF_DATA);
		bb.putInt(SIGNATURE);
		bb.putInt(V0);
		bb.putInt(masterKey.length);
		bb.put(masterKey);
		bb.putInt(1);
		bb.rewind();
		fc.position(0);
		fc.write(bb);
		
		this.fileInfo = new FileInfo(lf, masterKey, V0);						// Scrittura header
		this.fileInfo.write(generateRandomBuffer(numOfBuffs), 0);				// Scrittura buffer da leggere
		
		this.byteWrited = numOfBuffs * BUFF_CAPACITY;
		
	}

	@After
	public void tearDown() throws Exception {
		fc.close();
		fileInfo.close(true);
		lf.delete();
	}

	@Parameters
	public static Collection<?> getParameters() {

		return Arrays.asList(new Object[][] {
				// Test suite minimale
			
//			buffer, start, bestEffort, expectedException, bufferReaderCapacity			
			{ByteBuffer.allocate(5120), 0, false, null, 5120},
			{ByteBuffer.allocate(5120), 1, true, null, 5120}, 
			{ByteBuffer.allocate(0), 0, true, null, 0}, 
			{null, -1, false, NullPointerException.class, 0},
			
//				// Added for jacoco
			{ByteBuffer.allocate(5120), 6145, false, ShortReadException.class, 5120}, 
			{ByteBuffer.allocate(5120), 6145, true, null, 5120} 
			
			
		});
	}
	

	
	@Test
	public void testReadEntry() throws IOException {
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		
		int byteReaded = fileInfo.read(buffer, start, bestEffort);					// Lettura file 
		System.out.println("ByteWrited: "+byteWrited);
		System.out.println("ByteReaded: "+byteReaded);
		
		if (byteWrited <= bufferReaderCapacity-start) {
			System.out.println(byteWrited + " <= " + bufferReaderCapacity);
			assertEquals(byteWrited-start, byteReaded);
		}
		
		if(bufferReaderCapacity-start < 0){
			assertEquals(0, byteReaded);
		}else {
			System.out.println(String.format("%d > %d ----> Readed %d bytes", byteWrited, bufferReaderCapacity, byteReaded));
			assertEquals(bufferReaderCapacity-start, byteReaded);
		}
	}
		

	private static ByteBuffer[] generateRandomBuffer(int buff_lenght) {
		ByteBuffer[] buffer = new ByteBuffer[buff_lenght];
		for (int i = 0; i < buff_lenght; i++)
			buffer[i] = ByteBuffer.allocate(BUFF_CAPACITY);
		
		return buffer;
	}
}
