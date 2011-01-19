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

import com.google.protobuf.ByteString;

/** Iterator for folding over keys */
public interface KeyIter<T> {

	/**
	 * @param key The entry's Key
	 * @param tstamp Time stamp of the entry (lower 32-bit of Nanos at write time)
	 * @param entry_pos Position of this entry in the data file
	 * @param entry_size Size of the entry in the data file
	 * @param acc Accumulator for fold operations
	 * @return accumulator for next iteration
	 * 
	 * @see BitCaskFile#fold_keys(KeyIter, Object)
	 */

	T each(ByteString key, int tstamp, long entry_pos, int entry_size, T acc);
	
}
