package org.springframework.batch.core;

/**
 * Exception for {@link Job} to signal that some {@link JobParameters} are
 * invalid.
 * 
 * @author Dave Syer
 * 
 */
@SuppressWarnings("serial")
public class JobParametersInvalidException extends JobExecutionException {

	public JobParametersInvalidException(String msg) {
		super(msg);
	}

}
