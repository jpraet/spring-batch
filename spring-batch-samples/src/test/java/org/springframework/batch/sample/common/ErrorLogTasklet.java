package org.springframework.batch.sample.common;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

/**
 * @author Dan Garrette
 * @since 2.0
 */
public class ErrorLogTasklet implements Tasklet, StepExecutionListener {

	protected final Log logger = LogFactory.getLog(getClass());

	private JdbcOperations jdbcTemplate;

	private String jobName;

	private StepExecution stepExecution;

	private String stepName;

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		Assert.notNull(this.stepName, "Step name not set.  Either this class was not registered as a listener "
				+ "or the key 'stepName' was not found in the Job's ExecutionContext.");
		this.jdbcTemplate.update("insert into ERROR_LOG values (?, ?, '"+getSkipCount()+" records were skipped!')",
				jobName, stepName);
		return RepeatStatus.FINISHED;
	}

	/**
	 * @return
	 */
	private int getSkipCount() {
		if (stepExecution == null || stepName == null) {
			return 0;
		}
		for (StepExecution execution : stepExecution.getJobExecution().getStepExecutions()) {
			if (execution.getStepName().equals(stepName)) {
				return execution.getSkipCount();
			}
		}
		return 0;
	}

	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public void beforeStep(StepExecution stepExecution) {
		this.jobName = stepExecution.getJobExecution().getJobInstance().getJobName().trim();
		this.stepName = (String) stepExecution.getJobExecution().getExecutionContext().get("stepName");
		this.stepExecution = stepExecution;
		stepExecution.getJobExecution().getExecutionContext().remove("stepName");
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		return null;
	}

}
