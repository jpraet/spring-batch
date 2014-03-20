package org.springframework.batch.core.scope;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.beans.factory.InitializingBean;

public class StepStartupRunner implements InitializingBean {

	private Step step;
	
	public void setStep(Step step) {
		this.step = step;
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		StepExecution stepExecution = new StepExecution("step", new JobExecution(1L), 0L);
		step.execute(stepExecution);
		// expect no errors
	}

}
