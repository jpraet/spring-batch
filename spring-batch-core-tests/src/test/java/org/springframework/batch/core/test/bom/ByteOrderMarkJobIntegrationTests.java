/*
 * Copyright 2006-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.core.test.bom;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * http://docs.oracle.com/javase/7/docs/technotes/guides/intl/encoding.doc.html
 * http://docs.oracle.com/javase/7/docs/api/java/nio/charset/Charset.html
 * http://www.unicode.org/faq/utf_bom.html#BOM
 * 
 * @author Jimmy Praet
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/simple-job-launcher-context.xml",
		"/META-INF/batch/byteOrderMarkJob.xml" })
public class ByteOrderMarkJobIntegrationTests {

	/** Logger */
	private final Log logger = LogFactory.getLog(getClass());

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;

	@Test
	public void testLaunchJobUTF8() throws Exception {
		testByteOrderMark("UTF-8");
	}
	
	@Test
	public void testLaunchJobUTF16() throws Exception {
		testByteOrderMark("UTF-16");
	}

	@Test
	public void testLaunchJobUTF16BE() throws Exception {
		testByteOrderMark("UTF-16BE");
	}

	@Test
	public void testLaunchJobUTF16LE() throws Exception {
		testByteOrderMark("UTF-16LE");
	}

	@Test
	public void testLaunchJobUTF32() throws Exception {
		testByteOrderMark("UTF-32");
	}

	@Test
	public void testLaunchJobUTF32BE() throws Exception {
		testByteOrderMark("UTF-32BE");
	}

	@Test
	public void testLaunchJobUTF32LE() throws Exception {
		testByteOrderMark("UTF-32LE");
	}

	@Test
	public void testLaunchJobXUTF32BEBOM() throws Exception {
		testByteOrderMark("x-UTF-32BE-BOM");
	}	

	@Test
	public void testLaunchJobXUTF32LEBOM() throws Exception {
		testByteOrderMark("x-UTF-32LE-BOM");
	}	

	@Test
	public void testLaunchJobUnicodeBig() throws Exception {
		testByteOrderMark("UnicodeBig");
	}	

	@Test
	public void testLaunchJobUnicodeLittle() throws Exception {
		testByteOrderMark("UnicodeLittle");
	}	

	public void testByteOrderMark(String encoding) throws Exception {
		JobExecution execution = jobLauncher.run(job,
				new JobParametersBuilder().addString("encoding", encoding)
						.toJobParameters());
		assertEquals(BatchStatus.COMPLETED, execution.getStatus());
		String expected = FileUtils.readFileToString(new File(
				"src/test/resources/data/football/player-small.csv"), "UTF-8");
		String actual = FileUtils.readFileToString(new File(
				"target/bom-output.txt"), encoding);
		assertEquals("BOM bug for encoding: '" + encoding + "'", expected, actual);
	}

}
