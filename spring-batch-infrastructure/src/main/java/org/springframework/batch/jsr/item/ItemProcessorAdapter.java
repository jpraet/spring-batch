package org.springframework.batch.jsr.item;

import javax.batch.api.chunk.ItemProcessor;

import org.springframework.util.Assert;

public class ItemProcessorAdapter<I, O> implements org.springframework.batch.item.ItemProcessor<I, O> {

	private ItemProcessor delegate;

	public ItemProcessorAdapter(ItemProcessor processor) {
		Assert.notNull(processor, "An ItemProcessor implementation is required");
		this.delegate = processor;
	}

	@SuppressWarnings("unchecked")
	@Override
	public O process(I item) throws Exception {
		return (O) delegate.processItem(item);
	}
}
