package org.springframework.batch.core.test.skiplimit;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.file.LineMapper;

public class SkipLimitLineMapper implements LineMapper<Long> {
	
	private List<Long> readFailureRecords = new ArrayList<Long>();

	@Override
	public Long mapLine(String line, int lineNumber) throws Exception {
		Long item = Long.valueOf(line);
		if (readFailureRecords.contains(item)) {
			throw new SkippableException("skippable read failure");
		}
		return item;
	}
	
	public void setReadFailureRecords(List<Long> readFailureRecords) {
		this.readFailureRecords = readFailureRecords;
	}

	public void clear() {
		readFailureRecords = new ArrayList<Long>();
	}

}
