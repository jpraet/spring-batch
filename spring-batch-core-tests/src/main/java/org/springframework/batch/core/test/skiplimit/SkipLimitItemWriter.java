package org.springframework.batch.core.test.skiplimit;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemWriter;

public class SkipLimitItemWriter implements ItemWriter<Long> {

	private List<Long> writeFailureRecords = new ArrayList<Long>();
	
	private List<Long> written = new ArrayList<Long>();

	@Override
	public void write(List<? extends Long> items) throws Exception {
		for (Long writeFailureRecord : writeFailureRecords) {
			if (items.contains(writeFailureRecord)) {
				throw new SkippableException("skippable write failure");
			}
		}
		System.out.println("write: " + items);
		written.addAll(items);
	}
	
	public void clear() {
		writeFailureRecords = new ArrayList<Long>();
		written = new ArrayList<Long>();
	}

	public void setWriteFailureRecords(List<Long> writeFailureRecords) {
		this.writeFailureRecords = writeFailureRecords;
	}
	
	public List<Long> getWritten() {
		return written;
	}

}
