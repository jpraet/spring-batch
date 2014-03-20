/**
 * 
 */
package org.springframework.batch.core.explore.support;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.xml.DummyStep;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowStep;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.job.flow.support.StateTransition;
import org.springframework.batch.core.job.flow.support.state.EndState;
import org.springframework.batch.core.job.flow.support.state.StepState;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import test.jdbc.datasource.DataSourceInitializer;

/**
 * Integration test for the BATCH-2034 issue.
 * The {@link FlowStep} execution should not fail in the remote partitioning use case because the {@link SimpleJobExplorer}
 * doesn't retrieve the {@link JobInstance} from the {@link JobRepository}.
 * To illustrate the issue the test simulates the behavior of the {@code StepExecutionRequestHandler}
 * from the spring-batch-integration project.
 *  
 * @author Sergey Shcherbakov
 */
@ContextConfiguration(classes={SimpleJobExplorerIntegrationTests.Config.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class SimpleJobExplorerIntegrationTests {
	
	@Configuration
	@EnableBatchProcessing
	static class Config {
		
		@Autowired
		private StepBuilderFactory steps;

		@Bean
		public JobExplorer jobExplorer() throws Exception {
			return jobExplorerFactoryBean().getObject();
		}
		
		@Bean
		public JobExplorerFactoryBean jobExplorerFactoryBean() {
			JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
			jobExplorerFactoryBean.setDataSource(dataSource());
			return jobExplorerFactoryBean;
		}
		
		@Bean
		public Step flowStep() throws Exception {
			return steps.get("flowStep").flow(simpleFlow()).build();
		}

		@Bean
		public Step dummyStep() {
			return new DummyStep();
		}

		@Bean
		public SimpleFlow simpleFlow() {
			SimpleFlow simpleFlow = new SimpleFlow("simpleFlow");
			List<StateTransition> transitions = new ArrayList<StateTransition>();
			transitions.add(StateTransition.createStateTransition(new StepState(dummyStep()), "end0"));
			transitions.add(StateTransition.createEndStateTransition(new EndState(FlowExecutionStatus.COMPLETED, "end0")));
			simpleFlow.setStateTransitions(transitions);
			return simpleFlow;
		}
		
		@Bean
		public BasicDataSource dataSource() {
			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
			dataSource.setUrl("jdbc:hsqldb:mem:testdb;sql.enforce_strict_size=true");
			dataSource.setUsername("sa");
			dataSource.setPassword("");
			return dataSource;
		}
		
		@Bean
		public DataSourceInitializer dataSourceInitializer() {
			DataSourceInitializer dataSourceInitializer = new DataSourceInitializer();
			dataSourceInitializer.setDataSource(dataSource());
			dataSourceInitializer.setInitScripts(new Resource[] { 
				new ClassPathResource("org/springframework/batch/core/schema-drop-hsqldb.sql"),
				new ClassPathResource("org/springframework/batch/core/schema-hsqldb.sql")
			});
			return dataSourceInitializer;
		}
	}

	@Autowired
	private JobRepository jobRepository;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private FlowStep flowStep;
	
	@Test
	public void testGetStepExecution() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobInterruptedException, UnexpectedJobExecutionException {

		// Prepare the jobRepository for the test
		JobExecution jobExecution = jobRepository.createJobExecution("myJob", new JobParameters());
		StepExecution stepExecution = jobExecution.createStepExecution("flowStep");
		jobRepository.add(stepExecution);
		
		// Executed on the remote end in remote partitioning use case
		StepExecution jobExplorerStepExecution = jobExplorer.getStepExecution(jobExecution.getId(), stepExecution.getId());
		flowStep.execute(jobExplorerStepExecution);
		
		assertEquals(BatchStatus.COMPLETED, jobExplorerStepExecution.getStatus());
	}

}
