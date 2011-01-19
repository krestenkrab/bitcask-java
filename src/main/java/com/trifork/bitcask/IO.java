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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Utility class for I/O operations */
public class IO {

	public static long read(FileChannel ch, ByteBuffer[] bb,
			final long start_position) throws IOException {

		long position = start_position;
		for (int i = 0; i < bb.length; i++) {

			ByteBuffer buf = bb[i];
			while (buf.hasRemaining()) {

				int read = ch.read(buf, position);
				if (read >= 0) {
					position += read;

					if (read == 0) {
						Thread.yield();
					}
				} else {
					return 0;
				}
			}
		}

		return position - start_position;
	}

	public static long read(FileChannel ch, ByteBuffer bb,
			final long start_position) throws IOException {

		long position = start_position;
		while (bb.hasRemaining()) {

			int read = ch.read(bb, position);
			if (read >= 0) {
				position += read;

				if (read == 0) {
					Thread.yield();
				}
			} else {
				return 0;
			}
		}

		return position - start_position;
	}

	public static long write_fully(FileChannel ch, ByteBuffer[] vec)
			throws IOException {

		synchronized (ch) {

			long len = length(vec);

			long w = 0;

			while (w < len) {
				long ww = ch.write(vec);
				if (ww > 0) {
					w += ww;
				} else if (ww == 0) {
					Thread.yield();
				} else {
					return w;
				}
			}

			return w;
		}

	}

	private static long length(ByteBuffer[] vec) {
		long length = 0;
		for (int i = 0; i < vec.length; i++) {
			length += vec[i].remaining();
		}
		return length;
	}

}
