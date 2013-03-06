package org.springframework.batch.core.test.skiplimit;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/simple-job-launcher-context.xml",
		"/META-INF/batch/skipLimitJob.xml" })
public class SkipLimitIntegrationTests {

	/** Logger */
	private final Log logger = LogFactory.getLog(getClass());

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;

	@Autowired
	private SkipLimitLineMapper lineMapper;

	@Autowired
	private SkipLimitProcessor processor;

	@Autowired
	private SkipLimitItemWriter writer;
	
	private List<Long> allItems;

	@Before
	public void setUp() {
		writer.clear();
		lineMapper.clear();
		processor.clear();
		allItems = new ArrayList<Long>();
		for(long i=1; i<= 50; i++) {
			allItems.add(i);
		}
	}

	@Test
	public void testLaunchJobNoSkips() throws Exception {
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "no-skips")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(50, written.size());
	}
	
	@Test
	public void testLaunchJob1SkipInReader() throws Exception {
		lineMapper.setReadFailureRecords(Collections.singletonList(13L));
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "reader-1-skip")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(49, written.size());
	}	
	
	@Test
	public void testLaunchJob10SkipsInReader() throws Exception {
		lineMapper.setReadFailureRecords(Arrays.asList(7L, 12L, 13L, 19L, 33L, 35L, 36L, 37L, 44L, 50L));
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "reader-10-skips")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(40, written.size());
		System.out.println(execution.getStepExecutions());
	}	
	
	@Test
	public void testLaunchJobAllSkippedInReader() throws Exception {
		lineMapper.setReadFailureRecords(allItems);
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "reader-all-skipped")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(0, written.size());
	}	
	
	@Test
	public void testLaunchJob1SkipInProcessor() throws Exception {
		processor.setProcessFailureRecords(Collections.singletonList(13L));
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "processor-1-skip")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(49, written.size());
	}	
	
	@Test
	public void testLaunchJob10SkipsInProcessor() throws Exception {
		processor.setProcessFailureRecords(Arrays.asList(1L, 2L, 12L, 13L, 19L, 33L, 35L, 36L, 37L, 44L));
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "processor-10-skips")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(40, written.size());
	}	
	
	
	@Test
	public void testLaunchJobAllSkippedInProcessor() throws Exception {
		processor.setProcessFailureRecords(allItems);
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "processor-all-skipped")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(0, written.size());
	}	
	
	@Test
	public void testLaunchJob1SkipInWriter() throws Exception {
		writer.setWriteFailureRecords(Collections.singletonList(13L));
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "writer-1-skip")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(49, written.size());
	}	
	
	@Test
	public void testLaunchJob10SkipsInWriter() throws Exception {
		writer.setWriteFailureRecords(Arrays.asList(1L, 2L, 12L, 13L, 19L, 33L, 35L, 36L, 37L, 44L));
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "writer-10-skips")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(40, written.size());
	}	
	
	@Test
	public void testLaunchJobAllSkippedInWriter() throws Exception {
		writer.setWriteFailureRecords(allItems);
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("test", "writer-all-skipped")
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		List<Long> written = writer.getWritten();
		assertEquals(0, written.size());
	}	
	

}
