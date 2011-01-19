package com.trifork.bitcask;

import static net.java.quickcheck.generator.CombinedGenerators.byteArrays;
import static net.java.quickcheck.generator.PrimitiveGenerators.strings;
import static net.java.quickcheck.generator.CombinedGeneratorsIterables.somePairs;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import net.java.quickcheck.collection.Pair;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.ByteString;

public class BitCaskFileTest {

	private BitCaskFile data_file;
	private File data_dir;

	@org.junit.Before
	public void setupFile() throws Exception {

		data_dir = new File("/tmp/foo");
		data_dir.mkdirs();
		removeDataDir();

		data_file = BitCaskFile.create(data_dir, 1);
	}

	private void removeDataDir() throws IOException, InterruptedException {
		Process p = Runtime.getRuntime().exec(
				new String[] { "rm", "-Rf", "/tmp/foo" });
		p.waitFor();
	}

	@After
	public void cleanup() throws Exception {
		if (data_file != null) {
			data_file.close();
		}
		removeDataDir();
	}

	/** create a sample Data+Hint file, and compare the result of
	 * iterating through the keys of either 
	 * @throws Exception */
	@Test
	public void testHintFile() throws Exception {

		for (Pair<String, String> kv : somePairs(strings(), strings())) {
			data_file.write(ByteString.copyFromUtf8(kv.getFirst()),
							ByteString.copyFromUtf8(kv.getSecond()));
		}

		ArrayList<String> al1 = new ArrayList<String>();
		ArrayList<String> al2 = new ArrayList<String>();

		KeyIter<ArrayList<String>> iter = new KeyIter<ArrayList<String>>() {

			@Override
			public ArrayList<String> each(ByteString key, int tstamp, long off,
					int sz, ArrayList<String> acc) {

				acc.add(key.toStringUtf8() + ":" + tstamp + ":" + off + ":" + sz);

				return acc;

			}
		};

		data_file = BitCaskFile.open(data_dir, 1);
		data_file.fold_keys_datafile(iter, al1);
		data_file.fold_keys_hintfile(iter, al2);

		Assert.assertEquals(al1, al2);

		data_file.fold_keys_hintfile(new KeyIter<Void>() {

			@Override
			public Void each(ByteString key, int tstamp, long entryPos,
					int entrySize, Void acc) throws Exception {

				ByteString[] keyval = data_file.read(entryPos, entrySize);
				
				Assert.assertEquals(key, keyval[0]);
				
				return null;
			}
		}, null);
		
	}

}
