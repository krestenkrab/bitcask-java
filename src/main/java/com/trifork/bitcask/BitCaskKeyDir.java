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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.ByteString;

public class BitCaskKeyDir {

	Map<ByteString, BitCaskEntry> map = new HashMap<ByteString, BitCaskEntry>();
	ReadWriteLock rwl = new ReentrantReadWriteLock();

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

}
