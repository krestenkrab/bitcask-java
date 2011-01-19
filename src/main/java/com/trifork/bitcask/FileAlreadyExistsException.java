package com.trifork.bitcask;

import java.io.File;
import java.io.IOException;

public class FileAlreadyExistsException extends IOException {

	public FileAlreadyExistsException(File lockFilename) {
		super(lockFilename.getPath());
	}

}
