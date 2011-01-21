package com.trifork.bitcask;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.google.protobuf.ByteString;

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

	@Test
	public void foldTest() throws Exception {
		
		BitCask b = initDataset("/tmp/bc.test.fold", defaultDataset());
	
		final Map<ByteString,ByteString> l = new HashMap<ByteString, ByteString>();
		
		b.fold(new EntryIter<Void>() {

			@Override
			public Void each(ByteString key, ByteString value, int tstamp,
					long entryPos, int entrySize, Void acc) {
				
				l.put(key, value);
				return null;
				
			}
		}, null);
		
		assertEquals(l, defaultDataset());
		
		b.close();
	}
	
	private BitCask initDataset(String string,
			Map<ByteString, ByteString> ds) throws Exception {

		File dir = new File(string);
		rmdir(dir);
		
		BitCaskOptions opts = new BitCaskOptions();
		opts.read_write = true;
		BitCask b = BitCask.open(dir, opts);
		
		for (Map.Entry<ByteString, ByteString> ent : ds.entrySet()) {
			b.put(ent.getKey(), ent.getValue());
		}
		
		return b;
	}

	private Map<ByteString, ByteString> defaultDataset() {
		
		
		Map<ByteString, ByteString> res = new HashMap<ByteString, ByteString>();
		
		res.put(bs("k"), bs("v"));
		res.put(bs("k2"), bs("v2"));
		res.put(bs("k3"), bs("v3"));
		
		return res ;
		
	}

	private static ByteString bs(String string) {
		return ByteString.copyFromUtf8(string);
	}

	private void rmdir(File dir) throws IOException, InterruptedException {
		Process p = Runtime.getRuntime().exec(
				new String[] { "rm", "-Rf", dir.getPath() });
		p.waitFor();
	}
	
}
