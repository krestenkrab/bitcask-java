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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import com.google.protobuf.ByteString;

public class BitCaskLock {

	public static enum Stale {
		OK, NOT_STALE
	}

	public static enum Type {
		WRITE, MERGE;

		private  String type_name() {
			if (this == MERGE) return "merge";
			if (this == WRITE) return "write";
			throw new RuntimeException();
		}
	}

	private RandomAccessFile file;
	private boolean is_write_lock;

	private BitCaskLock(RandomAccessFile file, boolean isWriteLock) {
		this.file = file;
		this.is_write_lock = isWriteLock;
	}


	public static File read_activefile(Type type, File dirname) {
		File lock_filename = lock_filename(type, dirname);
		try {
			BitCaskLock lock = lock_acquire(lock_filename, false);

			try {
			String contents = lock.read_lock_data();
			
			int idx = contents.indexOf(' ');
			if (idx != -1) {
				String rest = contents.substring(idx + 1);
				
				int end = rest.indexOf('\n');
				if (end != -1) {
					String path = rest.substring(0, end);
					return new File(path);
				}
			}
			
			} finally {
				lock.release();
			}
			
		} catch (Exception e) {
		}

		return null;

	}

	public void write_activefile(Type type, File dirname) throws IOException {
		write_activefile(lock_filename(type, dirname));
	}
		
	private void write_activefile(File activeFilename) throws IOException {
		
		String lock_contents = 
			Integer.toString(OS.getpid()) + " " + activeFilename.getPath() + "\n";
		write_data(ByteString.copyFromUtf8(lock_contents));
		
	}
	
	public static BitCaskLock acquire(Type type, File dirname) throws IOException {
		File lock_filename = lock_filename(type, dirname);
		foo: do {
			try {
				BitCaskLock lock = lock_acquire(lock_filename, true);

				String lock_contents = Integer.toString(OS.getpid()) + " \n";
				lock.write_data(ByteString.copyFromUtf8(lock_contents));

				return lock;

			} catch (FileAlreadyExistsException e) {
				delete_stale_lock(lock_filename);
				continue foo;
			}
		} while (false);

		return null;
	}

	private void write_data(ByteString bytes) throws IOException {
		FileChannel ch = file.getChannel();
		if (is_write_lock) {
			ch.truncate(0);
			ch.write(bytes.asReadOnlyByteBuffer(), 0);

			return;
		}

		throw new IOException("file not writable");
	}

	public static Stale delete_stale_lock(Type type, File dirname) throws IOException {
		return delete_stale_lock(lock_filename(type, dirname));
	}
	
	private static Stale delete_stale_lock(File lockFilename) throws IOException {

		BitCaskLock l = null;

		try {
			l = lock_acquire(lockFilename, false);

		} catch (FileNotFoundException e) {
			return Stale.OK;
		} catch (IOException e) {
			return Stale.NOT_STALE;
		}

		try {

			int pid = l.read_lock_data_pid();

			if (OS.pid_exists(pid)) {
				return Stale.NOT_STALE;
			} else {
				lockFilename.delete();
				return Stale.OK;
			}

		} catch (IOException e) {
			return Stale.NOT_STALE;
		} finally {
			l.release();
		}

	}

	private int read_lock_data_pid() throws IOException {
		String data = read_lock_data();

		int idx = data.indexOf(' ');
		if (idx != -1) {
			return Integer.parseInt(data.substring(0, idx));
		}
		
		throw new IOException();
	}

	private String read_lock_data() throws IOException {
		this.file.seek(0);
		int len = (int) file.length();
		byte[] data = new byte[len];
		file.read(data);

		ByteString s = ByteString.copyFrom(data);
		String ss = s.toStringUtf8();	
		
		return ss;
	}

	public void release() throws IOException {
		if (file != null) {

			if (is_write_lock) {
				file.close();
			}
		}

		file = null;
	}

	private static BitCaskLock lock_acquire(File lockFilename, boolean is_write_lock)
			throws IOException {

		if (is_write_lock) {
			if (lockFilename.createNewFile() == false) {
				// file already exists, so we fail!

				throw new FileAlreadyExistsException(lockFilename);
			}
		}

		RandomAccessFile f = new RandomAccessFile(lockFilename,
				is_write_lock ? "rws" : "r");

		return new BitCaskLock(f, is_write_lock);
	}

	private static File lock_filename(Type type, File dirname) {
		return new File(dirname, "bitcask." + type.type_name() + ".lock");
	}

}
