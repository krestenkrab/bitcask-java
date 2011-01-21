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

		final Map<ByteString, ByteString> l = contents(b);

		assertEquals(l, defaultDataset());

		b.close();
	}

	@Test
	public void openTest() throws Exception {

		initDataset("/tmp/bc.test.open", defaultDataset()).close();

		BitCask b = BitCask.open(new File("/tmp/bc.test.open"),
				new BitCaskOptions());
		final Map<ByteString, ByteString> l = contents(b);

		assertEquals(l, defaultDataset());

		b.close();
	}

	@Test
	public void wrapTest() throws Exception {
		BitCaskOptions opts = new BitCaskOptions();
		opts.max_file_size = 1;
		initDataset("/tmp/bc.test.wrap", opts, defaultDataset()).close();

		BitCask b = BitCask.open(new File("/tmp/bc.test.wrap"),
				new BitCaskOptions());

		for (Map.Entry<ByteString, ByteString> ents : defaultDataset().entrySet()) {
			assertEquals(ents.getValue(), b.get(ents.getKey()));			
		}
		
		assertEquals(3, b.readable_files().length);
		
		b.close();
	}

	/** utility that slurps an entire bitcask into a HashMap */
	private Map<ByteString, ByteString> contents(BitCask b) throws IOException {
		final Map<ByteString, ByteString> l = new HashMap<ByteString, ByteString>();

		b.fold(new KeyValueIter<Void>() {

			@Override
			public Void each(ByteString key, ByteString value, Void acc) {

				l.put(key, value);
				return null;

			}
		}, null);
		return l;
	}

	private BitCask initDataset(String string, Map<ByteString, ByteString> ds)
			throws Exception {

		BitCaskOptions opts = new BitCaskOptions();

		return initDataset(string, opts, ds);
	}

	private BitCask initDataset(String string, BitCaskOptions opts,
			Map<ByteString, ByteString> ds) throws Exception {

		File dir = new File(string);
		rmdir(dir);

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

		return res;

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
