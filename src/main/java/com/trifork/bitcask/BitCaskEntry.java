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

public class BitCaskEntry {

	public final int tstamp;
	public final int file_id;
	public final long offset;
	public final int total_sz;

	public BitCaskEntry(int file_id, int ts, long offset, int total_sz) {
		this.file_id = file_id;
		this.tstamp = ts;
		this.offset = offset;
		this.total_sz = total_sz;
	}

	boolean is_newer_than(BitCaskEntry old) {
		return old.tstamp < tstamp
			|| ( old.tstamp == tstamp 
				 &&  ( old.file_id < file_id  
						 || (old.file_id == file_id 
								 && old.offset < offset)));
	}
}
