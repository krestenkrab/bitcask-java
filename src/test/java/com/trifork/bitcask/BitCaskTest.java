package com.trifork.bitcask;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class BitCaskTest {

	
	@Test
	public void roundtripTest() throws Exception {
		
		File dir = new File("/tmp/bc.test.roundtrip");
		rmdir(dir);
		
		BitCaskOptions opts = new BitCaskOptions();
		opts.read_write = true;
		BitCask b = BitCask.open(dir, opts);
		
		b.put("k", "v");		
		assertEquals(b.getString("k"), "v");
		
		b.put("k2", "v2");
		b.put("k", "v3");
		
		assertEquals(b.getString("k2"), "v2");
		assertEquals(b.getString("k"), "v3");
		
		b.close();
	}

	private void rmdir(File dir) throws IOException, InterruptedException {
		Process p = Runtime.getRuntime().exec(
				new String[] { "rm", "-Rf", dir.getPath() });
		p.waitFor();
	}
	
}
