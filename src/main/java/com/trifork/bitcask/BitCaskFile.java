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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;

public class BitCaskFile {

	static final BitCaskFile FRESH_FILE = new BitCaskFile();
	
	public enum WriteCheck {
		WRAP, FRESH, OK
	}

	// 4+4+2+4
	private static final int HEADER_SIZE = 14;

	// 4+2+4+8
	private static final int HINT_HEADER_SIZE = 18;

	FileChannel wch;
	FileChannel rch;

	FileChannel wch_hint;

	private AtomicLong write_offset;
	final File filename;
	final int file_id;

	private BitCaskFile(int file_id, File filename, FileChannel wch, FileChannel wch_hint,
			FileChannel rch) throws IOException {
		this.file_id = file_id;
		this.filename = filename;
		this.wch = wch;
		this.rch = rch;
		this.wch_hint = wch_hint;
		this.write_offset = new AtomicLong(wch.position());
	}
	
	public BitCaskFile() {
		this.filename = null;
		this.file_id = -1;
	}

	public ByteString[] read(long offset, int length) throws IOException {
		
		byte[] header = new byte[HEADER_SIZE];

		ByteBuffer h = ByteBuffer.wrap(header);
		long read = IO.read(rch, h, offset);
		if (read != HEADER_SIZE) {
			throw new IOException("cannot read header @ 0x"+Long.toHexString(offset));
		}

		int crc32 = h.getInt(0);
		/* int tstamp = h.getInt(); */ 
		int key_len = h.getChar(8);
		int val_len = h.getInt(10);

		int key_val_size = key_len + val_len;
		
		if (length != (HEADER_SIZE + key_val_size)) {
			throw new IOException("bad entry size");
		}
		
		byte[] kv = new byte[key_val_size];
		ByteBuffer key_val = ByteBuffer.wrap(kv);

		long kv_pos = offset + HEADER_SIZE;
		read = IO.read(rch, key_val, kv_pos);
		if (read != key_val_size) {
			throw new IOException("cannot read key+value @ 0x"+Long.toHexString(offset));
		}

		CRC32 crc = new CRC32();
		crc.reset();
		crc.update(header, 4, HEADER_SIZE - 4);
		crc.update(kv);

		if (crc.getValue() != crc32) {
			throw new IOException("Mismatching CRC code");
		}

		ByteString[] result = new ByteString[] {
				ByteString.copyFrom(kv, 0, key_len),
				ByteString.copyFrom(kv, key_len, val_len)
		};
		
		return result;
	}
	
	public BitCaskEntry write(ByteString key, ByteString value) throws IOException {

		int tstamp = tstamp();
		int key_size = key.size();
		int value_size = value.size();

		ByteBuffer[] vec = file_entry(key, value, tstamp, key_size, value_size);

		int entry_size = HEADER_SIZE + key_size + value_size;
		long entry_pos = write_offset.getAndAdd(entry_size);
		IO.write_fully(wch, vec);

		ByteBuffer[] hfe = hint_file_entry(key, tstamp, entry_pos,
				entry_size);
		IO.write_fully(wch_hint, hfe);

		return new BitCaskEntry(file_id, tstamp, entry_pos, entry_size);
	}

	private ByteBuffer[] file_entry(ByteString key, ByteString value, int tstamp,
			int key_size, int value_size) {
		byte[] header = new byte[HEADER_SIZE];
		ByteBuffer h = ByteBuffer.wrap(header);

		ByteBuffer k = key.asReadOnlyByteBuffer();
		ByteBuffer v = value.asReadOnlyByteBuffer();

		h.putInt(4, tstamp);
		h.putShort(8, (short) key_size);
		h.putInt(10, value_size);

		CRC32 crc = new CRC32();
		crc.update(header, 4, 10);
		crc.update(key);
		crc.update(value);
		long crc_value = crc.getValue();

		h.putInt(0, (int) crc_value);

		ByteBuffer[] vec = new ByteBuffer[] { h, k, v };
		return vec;
	}

	private ByteBuffer[] hint_file_entry(ByteString key, int tstamp,
			long entry_offset, int entry_size) {

		byte[] header = new byte[HINT_HEADER_SIZE];
		ByteBuffer h = ByteBuffer.wrap(header);

		h.putInt(0, tstamp);
		h.putShort(4, (short) key.size());
		h.putInt(6, entry_size);
		h.putLong(10, entry_offset);

		return new ByteBuffer[] { h, key.asReadOnlyByteBuffer() };
	}

	/** in bitcask, timestamp is the #seconds in the system */
	static int tstamp() {
		return (int) (System.currentTimeMillis() / 1000L);
	}

	
	/** open existing bitcask file in given directory 
	 * @throws IOException */
	public static BitCaskFile open(File dirname, int tstamp)
			throws IOException {

		File filename = mk_filename(dirname, tstamp);
		return open(filename);
	}
	
	public static BitCaskFile open(File filename) throws IOException {
		
		int tstamp = BitCaskFile.tstamp(filename);
		
		FileChannel wch = new FileOutputStream(filename, true).getChannel();
		FileChannel wch_hint = new FileOutputStream(hint_filename(filename),
				true).getChannel();
		
		

		FileChannel rch = new RandomAccessFile(filename, "r").getChannel();

		return new BitCaskFile(tstamp, filename, wch, wch_hint, rch);
	}

	/** Create a new bitcask file in named directory */
	static BitCaskFile create(File dirname) throws IOException {
		return create(dirname, tstamp());
	}
	
	/** Create a new bitcask file in named directory */
	static BitCaskFile create(File dirname, int tstamp) throws IOException {
		ensuredir(dirname);

		boolean created = false;

		File filename = null;
		while (!created) {
			filename = mk_filename(dirname, tstamp);
			created = filename.createNewFile();
			if (!created) {
				tstamp += 1;
			}
		}

		FileChannel wch = new FileOutputStream(filename, true).getChannel();
		FileChannel wch_hint = new FileOutputStream(hint_filename(filename),
				true).getChannel();

		FileChannel rch = new RandomAccessFile(filename, "r").getChannel();

		return new BitCaskFile(tstamp, filename, wch, wch_hint, rch);
	}

	/** Fold over all entries in this bitcask file */
	public <T> T fold(EntryIter<T> iter, T acc) throws IOException {

		CRC32 crc = new CRC32();

		byte[] header = new byte[HEADER_SIZE];
		long pos = 0;
		while (pos < write_offset.get()) {
			ByteBuffer h = ByteBuffer.wrap(header);
			long read = IO.read(rch, h, pos);
			if (read != HEADER_SIZE) {
				return acc;
			}

			h.rewind();
			int crc32 = h.getInt();
			int tstamp = h.getInt();
			int key_len = h.getChar();
			int val_len = h.getInt();

			byte[] kv = new byte[key_len + val_len];
			ByteBuffer key_val = ByteBuffer.wrap(kv);

			long kv_pos = pos + HEADER_SIZE;
			read = IO.read(rch, key_val, kv_pos);
			if (read != (key_len + val_len)) {
				return acc;
			}

			crc.reset();
			crc.update(header, 4, HEADER_SIZE - 4);
			crc.update(kv);

			if (crc.getValue() != crc32) {
				throw new IOException("Mismatching CRC code");
			}

			int entry_length = HEADER_SIZE + key_len + val_len;
			acc = iter.each(ByteString.copyFrom(kv, 0, key_len), 
							ByteString.copyFrom(kv, key_len, val_len), tstamp, pos, entry_length, acc);
		}

		return acc;

	}

	/** return true if this bitcask file has a hint file */
	public boolean hasHintfile() {
		return hint_filename(filename).canRead();
	}

	/** Fold keys (use hint file if it exists) */
	public <T> T fold_keys(KeyIter<T> iter, T acc) throws Exception {
		if (hasHintfile()) {
			return fold_keys_hintfile(iter, acc);
		} else {
			return fold_keys_datafile(iter, acc);
		}
	}

	/** Fold keys by reading through the hintfile 
	 * @throws Exception */
	public <T> T fold_keys_hintfile(KeyIter<T> iter, T acc) throws Exception {

		FileInputStream fi;
		FileChannel ch = (fi = new FileInputStream(hint_filename(filename)))
				.getChannel();

		try {
			byte[] header = new byte[HINT_HEADER_SIZE];
			while (true) {
				ByteBuffer h = ByteBuffer.wrap(header);
				long read = ch.read(h);
				if (read != HINT_HEADER_SIZE) {
					return acc;
				}

				h.rewind();
				int tstamp = h.getInt();
				int key_len = h.getChar();
				int entry_len = h.getInt();
				long entry_off = h.getLong();

				byte[] k = new byte[key_len];
				ByteBuffer key = ByteBuffer.wrap(k);

				read = ch.read(key);
				if (read != key_len) {
					return acc;
				}

				acc = iter.each(ByteString.copyFrom(k), tstamp, entry_off, entry_len, acc);
			}
		} finally {
			fi.close();
		}
	}

	/** Fold keys by reading through the data file */
	public <T> T fold_keys_datafile(KeyIter<T> iter, T acc) throws Exception {

		byte[] header = new byte[HEADER_SIZE];
		long pos = 0;
		while (true) {
			ByteBuffer h = ByteBuffer.wrap(header);
			long read = IO.read(rch, h, pos);
			if (read != HEADER_SIZE) {
				return acc;
			}

			h.rewind();
			h.getInt(); // skip crc32
			int tstamp = h.getInt();
			int key_len = h.getChar();
			int val_len = h.getInt();

			byte[] k = new byte[key_len];
			ByteBuffer key = ByteBuffer.wrap(k);

			read = IO.read(rch, key, pos + HEADER_SIZE);
			if (read != key_len) {
				return acc;
			}

			int entry_size = HEADER_SIZE + key_len + val_len;
			acc = iter.each(ByteString.copyFrom(k), tstamp, pos, entry_size, acc);

			pos += entry_size;
		}
	}

	/** Close for writing */
	public synchronized void close_for_writing() throws IOException {
		if (wch != null) {
			wch.close();
			wch = null;
		}
		if (wch_hint != null) {
			wch_hint.close();
			wch = null;
		}
	}

	/** Close for reading and writing */
	public synchronized void close() throws IOException {
		close_for_writing();
		if (rch != null) {
			rch.close();
		}
	}

	/** Create-if-not-exists for directory or fail */
	static void ensuredir(File dirname) {
		if (dirname.exists() && dirname.isDirectory())
			return;
		if (!dirname.mkdirs())
			throw new RuntimeException("cannot create " + dirname);
	}

	/** 
	 * Given a directory and a timestamp, construct a data file name.
	 */
	static File mk_filename(File dirname, int tstamp) {
		return new File(dirname, "" + tstamp + ".bitcask.data");
	}

	/**
	 * Given the name of the data file (filename), construct the name of the
	 * corresponding hint file.
	 */
	private static File hint_filename(File filename) {
		File parent = filename.getParentFile();
		String name = filename.getName();

		if (name.endsWith(".data")) {
			return new File(parent, name.substring(0, name.length() - 5)
					+ ".hint");
		} else {
			return new File(parent, name + ".hint");
		}
	}

	public WriteCheck check_write(ByteString key, ByteString value, long maxFileSize) {
		if (file_id == -1)
			return WriteCheck.FRESH;

		int size = HEADER_SIZE + key.size() + value.size();
		
		if (write_offset.get() + size > maxFileSize) {
			return WriteCheck.WRAP;
		} else {
			return WriteCheck.OK;
		}
	}

	public static int tstamp(File file) {
		String name = file.getName();
		int idx = name.indexOf('.');
		int val = Integer.parseInt(name.substring(0, idx));
		return val;
	}



}
