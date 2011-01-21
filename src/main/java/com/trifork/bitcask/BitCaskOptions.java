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

public class BitCaskOptions {

	public int expiry_secs = 0;
	public long max_file_size = 1024 * 1024; /* 1mb file size */
	public boolean read_write = false;
	public int open_timeout_secs = 20;

	public int expiry_time() {
		if (expiry_secs > 0) 
			return BitCaskFile.tstamp() - expiry_secs;
		else
			return 0;
	}

	public boolean is_read_write() {
		return read_write;
	}
		
	
	
}
