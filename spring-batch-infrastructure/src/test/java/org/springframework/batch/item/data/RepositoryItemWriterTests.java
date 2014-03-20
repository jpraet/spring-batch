package org.springframework.batch.item.data;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.repository.CrudRepository;

public class RepositoryItemWriterTests {

	@Mock
	private CrudRepository<String, Serializable> repository;

	private RepositoryItemWriter<String> writer;

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		writer = new RepositoryItemWriter<String>();
		writer.setMethodName("save");
		writer.setRepository(repository);
	}

	@Test
	public void testAfterPropertiesSet() throws Exception {
		writer.afterPropertiesSet();

		writer.setRepository(null);

		try {
			writer.afterPropertiesSet();
			fail();
		} catch (IllegalStateException e) {
		}
	}

	@Test
	public void testWriteNoItems() throws Exception {
		writer.write(null);

		writer.write(new ArrayList<String>());

		verifyZeroInteractions(repository);
	}

	@Test
	public void testWriteItems() throws Exception {
		List<String> items = Collections.singletonList("foo");

		writer.write(items);

		verify(repository).save("foo");
	}
}
