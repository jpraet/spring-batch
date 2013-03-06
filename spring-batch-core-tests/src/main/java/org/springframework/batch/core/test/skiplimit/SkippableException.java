package org.springframework.batch.core.test.skiplimit;

public class SkippableException extends RuntimeException {
	
	public SkippableException(String msg) {
		super(msg);
	}

}
