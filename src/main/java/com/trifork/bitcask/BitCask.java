/**
 * This file is part of Bitcask Java
 *
 * Copyright (c) 2011 by Trifork
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.trifork.bitcask;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.protobuf.ByteString;
import com.trifork.bitcask.BitCaskLock.Type;

public class BitCask {

	private static final ByteString TOMBSTONE = ByteString
			.copyFromUtf8("bitcask_tombstone");

	/** bc_state */
	File dirname;
	BitCaskFile write_file = BitCaskFile.FRESH_FILE;
	BitCaskLock write_lock;
	Map<File, BitCaskFile> read_files = new HashMap<File, BitCaskFile>();
	long max_file_size;
	BitCaskOptions opts;
	BitCaskKeyDir keydir;

	public static BitCask open(File dirname, BitCaskOptions opts)
			throws Exception {
		BitCask result = new BitCask();

		BitCaskFile.ensuredir(new File(dirname, "bitcask"));

		if (opts.is_read_write()) {
			BitCaskLock.delete_stale_lock(Type.WRITE, dirname);
			result.write_file = BitCaskFile.FRESH_FILE;
		}

		result.dirname = dirname;
		
		BitCaskKeyDir keydir;
		keydir = BitCaskKeyDir.keydir_new(dirname, opts.open_timeout_secs);
		result.keydir = keydir;
		if (!keydir.is_ready()) {			
			File[] files = result.readable_files();
			BitCask.scan_key_files(files, keydir);
			keydir.mark_ready();
		}

		result.max_file_size = opts.max_file_size;
		result.dirname = dirname;
		result.opts = opts;

		return result;
	}
	
	
	public void close() throws IOException {
		
		// release?
		keydir = null;
		
		for (BitCaskFile read_file : read_files.values()) {
			read_file.close();
		}
		
		read_files.clear();
		
		if (write_file == null || write_file == BitCaskFile.FRESH_FILE) {
			// ok
		} else {
			write_file.close();
			write_lock.release();
		}
	}

	public void put(ByteString key, ByteString value) throws IOException {
		if (write_file == null) {
			throw new IOException("read only");
		}

		switch (write_file.check_write(key, value, max_file_size)) {
		case WRAP: {
			write_file.close_for_writing();
			BitCaskFile last_write_file = write_file;
			BitCaskFile nwf = BitCaskFile.create(dirname);
			write_lock.write_activefile(nwf);

			write_file = nwf;
			read_files.put(last_write_file.filename, last_write_file);
			break;
		}

		case FRESH:
			// time to start our first write file
		{
			BitCaskLock wl = BitCaskLock.acquire(Type.WRITE, dirname);
			BitCaskFile nwf = BitCaskFile.create(dirname);
			wl.write_activefile(nwf);

			this.write_lock = wl;
			this.write_file = nwf;

			break;
		}

		case OK:
			// we're good to go
		}

		BitCaskEntry entry = write_file.write(key, value);
		keydir.put(key, entry);
	}

	public ByteString get(ByteString key) throws IOException {
		return get(key, 2);
	}

	private ByteString get(ByteString key, int try_num) throws IOException {
		BitCaskEntry entry = keydir.get(key);
		if (entry == null) {
			return null;
		}

		if (entry.tstamp < opts.expiry_time()) {
			return null;
		}

		BitCaskFile file_state = get_filestate(entry.file_id);
		/** merging deleted file between keydir.get and here */
		if (file_state == null) {
			Thread.yield();
			return get(key, try_num - 1);
		}

		ByteString[] kv = file_state.read(entry.offset, entry.total_sz);

		if (kv[1].equals(TOMBSTONE)) {
			return null;
		} else {
			return kv[1];
		}
	}

	private BitCaskFile get_filestate(int fileId) throws IOException {

		File fname = BitCaskFile.mk_filename(dirname, fileId);
		BitCaskFile f = read_files.get(fname);
		if (f != null) {
			return f;
		}

		f = BitCaskFile.open(dirname, fileId);
		read_files.put(fname, f);

		return f;
	}

	private static final Comparator<? super File> REVERSE_DATA_FILE_COMPARATOR = new Comparator<File>() {

		@Override
		public int compare(File file0, File file1) {
			int i0 = BitCaskFile.tstamp(file0);
			int i1 = BitCaskFile.tstamp(file1);

			if (i0 < i1)
				return 1;
			if (i0 == i1)
				return 0;

			return -1;
		}
	};

	private File[] readable_files() {

		final File writing_file = BitCaskLock.read_activefile(Type.WRITE,
				dirname);
		final File merging_file = BitCaskLock.read_activefile(Type.MERGE,
				dirname);

		return list_data_files(writing_file, merging_file);
	}

	static Pattern DATA_FILE = Pattern.compile("[0-9]+.bitcask.data");

	private File[] list_data_files(final File writing_file,
			final File merging_file) {
		File[] files = dirname.listFiles(new FileFilter() {
			@Override
			public boolean accept(File f) {
				if (f == writing_file || f == merging_file)
					return false;

				return DATA_FILE.matcher(f.getName()).matches();
			}
		});

		Arrays.sort(files, 0, files.length, REVERSE_DATA_FILE_COMPARATOR);

		return files;
	}

	static void scan_key_files(File[] files, final BitCaskKeyDir keydir)
			throws Exception {

		for (File f : files) {

			final BitCaskFile file = BitCaskFile.open(f);

			file.fold_keys(new KeyIter<Void>() {
				@Override
				public Void each(ByteString key, int tstamp, long entryPos,
						int entrySize, Void acc) throws Exception {

					keydir.put(key, new BitCaskEntry(file.file_id, tstamp,
							entryPos, entrySize));

					return null;
				}
			}, null);

		}
	}

	public void put(String key, String value) throws IOException {
		put(ByteString.copyFromUtf8(key), ByteString.copyFromUtf8(value));
	}

	public String getString(String key) throws IOException {
		ByteString val = get(ByteString.copyFromUtf8(key));
		if (val == null)
			return null;
		return val.toStringUtf8();
	}


}
