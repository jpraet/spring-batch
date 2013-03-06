package org.springframework.batch.core.test.skiplimit;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

public class SkipLimitProcessor implements ItemProcessor<Long, Long> {
	
	private List<Long> processFailureRecords = new ArrayList<Long>();
	
	@Override
	public Long process(Long item) throws Exception {
		if (processFailureRecords.contains(item)) {
			throw new SkippableException("skippable process failure");
		}
		return item;	
	}
	
	public void setProcessFailureRecords(List<Long> processFailureRecords) {
		this.processFailureRecords = processFailureRecords;
	}

	public void clear() {
		processFailureRecords = new ArrayList<Long>();
	}

}
