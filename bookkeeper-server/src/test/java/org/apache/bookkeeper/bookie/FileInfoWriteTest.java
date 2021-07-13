package org.apache.bookkeeper.bookie;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
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
public class FileInfoWriteTest {

	private Class<? extends Exception> expectedException;
	private FileInfo fileInfo;
	private FileChannel fc;
	private File lf;
	private ByteBuffer[] buffer;
	private long position;
	private long expectedWritedBytes;
	
	/*
	 * Header signature
	 */
	public static final int SIGNATURE = ByteBuffer.wrap("BKLE".getBytes(UTF_8)).getInt();	
	static final long START_OF_DATA = 1024;
	private static byte[] masterKey = "masterKey".getBytes();
	private static int V0 = 0; 															// Accepted versions for headers: {V0, V1}
	private static int buff_lenght = 3;


	@Rule
	public ExpectedException exceptionRule = ExpectedException.none();

	public FileInfoWriteTest(ByteBuffer[] buffer, long position, long expectedWritedBytes, Class<? extends Exception> expectedException) {
		this.buffer = buffer;
		this.position = position;
		this.expectedWritedBytes = expectedWritedBytes;
		this.expectedException = expectedException;
	}

	@Before
	public void setUp() throws Exception {
		this.lf = new File("ledgerIndexFile.txt");						// Creazione file indice fittizio
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
		fc.write(bb);													// Scrittura header su file indice
		
		fileInfo = new FileInfo(lf, masterKey, V0);
	}

	@After
	public void tearDown() throws Exception {
			fc.close();
			fileInfo.close(true);
			lf.delete();
	}

	@Parameters
	public static Collection<?> getParameters() {

		ByteBuffer[] buffer = generateRandomBuffer(buff_lenght);
		ByteBuffer[] newBuffer = generateRandomBuffer(buff_lenght);
		ByteBuffer[] empty = generateRandomBuffer(0);
		

		
		return Arrays.asList(new Object[][] {
				// Test suite minimale
				
				// buffer, position, expectedWritedBytes, expectedException
				{ buffer, 1 , buffer.length*1024, null},
				{ newBuffer, 0, newBuffer.length*1024, null},
				{ empty, -1, 0, IndexOutOfBoundsException.class },
				{ null, 1, 0, NullPointerException.class },
				
				

		});
	}
	
	/*
	 * Allocazione array ByteBuffer[]
	 */
	private static ByteBuffer[] generateRandomBuffer(int buff_lenght) {
		ByteBuffer[] buffer = new ByteBuffer[buff_lenght];
		for (int i = 0; i < buff_lenght; i++)
			buffer[i] = ByteBuffer.allocate(1024);
		
		return buffer;
	}

	@Test
	public void testReadEntry() throws IOException {
		if (expectedException != null) {
			exceptionRule.expect(expectedException);
		}
		long result = fileInfo.write(buffer, position);							// Scrittura buffer su file 
		System.out.println("result: "+result + " = " + expectedWritedBytes);
		assertEquals(expectedWritedBytes, result);								// Verifica quantità di byte scritti
		
		/*
		 * Added for pit mutation
		 * Verifica dimensione complessiva file [header + dati]
		 */
		FileChannel fc_2 = FileChannel.open(fileInfo.getLf().getAbsoluteFile().toPath(), StandardOpenOption.READ);
		assertEquals((buff_lenght+1)*1024+position,fc_2.size());				
	}

}
