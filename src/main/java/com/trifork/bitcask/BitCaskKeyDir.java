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
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.ByteString;

public class BitCaskKeyDir {

	Map<ByteString, BitCaskEntry> map = new HashMap<ByteString, BitCaskEntry>();
	ReadWriteLock rwl = new ReentrantReadWriteLock();
	private boolean is_ready;

	public boolean put(ByteString key, BitCaskEntry ent) {

		Lock writeLock = rwl.writeLock();
		writeLock.lock();
		try {

			BitCaskEntry old = map.get(key);
			if (old == null) {
				map.put(key, ent);
				return true;
			} else if (ent.is_newer_than(old)) {
				map.put(key, ent);
				return true;
			} else {
				return false;
			}

		} finally {
			writeLock.unlock();
		}

	}
	
	public BitCaskEntry get(ByteString key) {
		
		Lock readLock = rwl.readLock();
		readLock.lock();
		try {
			
			return map.get(key);
			
		} finally {
			readLock.unlock();
		}
		
	}

	public static Map<File,BitCaskKeyDir> key_dirs = new HashMap<File, BitCaskKeyDir>();
	public static Lock keydir_lock = new ReentrantLock();
	
	public static BitCaskKeyDir keydir_new(File dirname, int openTimeoutSecs) throws IOException {
		
		File abs_name = dirname.getAbsoluteFile();
		BitCaskKeyDir dir;
		keydir_lock.lock();
		try {
			
			dir = key_dirs.get(abs_name);
			if (dir == null) {
				dir = new BitCaskKeyDir();
				key_dirs.put(abs_name, dir);
				return dir;
			}
			
			
		} finally {
			keydir_lock.unlock();
		}

		if (dir.wait_for_ready(openTimeoutSecs)) {
			return dir;
		} else {
			throw new IOException("timeout while waiting for keydir");
		}
	}

	public synchronized boolean is_ready() {
		return is_ready;
	}
	
	public synchronized void mark_ready() {
		is_ready = true;
		this.notifyAll();
	}
	
	public synchronized boolean wait_for_ready(int timeout_secs) {
		long now = System.currentTimeMillis();
		long abs_timeout = now + (timeout_secs * 1000);
		
		while (!is_ready && now < abs_timeout) {
			try {
				wait();
			} catch (InterruptedException e) {
				// ignore
			}
		
			now = System.currentTimeMillis();
		}
		
		return is_ready;
	}
	

}
