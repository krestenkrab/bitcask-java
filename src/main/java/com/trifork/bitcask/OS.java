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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OS {

	public static int getpid() {

		RuntimeMXBean rtb = ManagementFactory.getRuntimeMXBean();
		String processName = rtb.getName();
		Integer pid = tryPattern1(processName);

		if (pid == null) {
			throw new UnsupportedOperationException("cannot get pid");
		}
		
		return pid.intValue();
	}

	/** from http://golesny.de/wiki/code:javahowtogetpid; that site has more suggestions if this fails...*/
	private static Integer tryPattern1(String processName) {
		Integer result = null;

		/* tested on: */
		/* - windows xp sp 2, java 1.5.0_13 */
		/* - mac os x 10.4.10, java 1.5.0 */
		/* - debian linux, java 1.5.0_13 */
		/* all return pid@host, e.g 2204@antonius */

		Pattern pattern = Pattern.compile("^([0-9]+)@.+$",
				Pattern.CASE_INSENSITIVE);
		Matcher matcher = pattern.matcher(processName);
		if (matcher.matches()) {
			result = new Integer(Integer.parseInt(matcher.group(1)));
		}
		return result;
		
		
	}

	public static boolean pid_exists(int pid) {
		// TODO: implement
		return false;
	}
}
