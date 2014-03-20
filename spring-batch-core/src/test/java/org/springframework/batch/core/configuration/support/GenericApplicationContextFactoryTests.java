/*
 * Copyright 2006-2007 the original author or authors.
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
package org.springframework.batch.core.configuration.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;

/**
 * @author Dave Syer
 * 
 */
public class GenericApplicationContextFactoryTests {

	@Test
	public void testCreateJob() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(
				new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(), "trivial-context.xml")));
		@SuppressWarnings("resource")
		ConfigurableApplicationContext context = factory.createApplicationContext();
		assertNotNull(context);
		assertTrue("Wrong id: " + context, context.getId().contains("trivial-context.xml"));
	}

	@Test
	public void testGetJobName() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(
				new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(), "trivial-context.xml")));
		assertEquals("test-job", factory.createApplicationContext().getBeanNamesForType(Job.class)[0]);
	}

	@SuppressWarnings("resource")
	@Test
	public void testParentConfigurationInherited() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(
				new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(), "child-context.xml")));
		factory.setApplicationContext(new ClassPathXmlApplicationContext(ClassUtils.addResourcePathToPackagePath(
				getClass(), "parent-context.xml")));
		ConfigurableApplicationContext context = factory.createApplicationContext();
		assertEquals("test-job", context.getBeanNamesForType(Job.class)[0]);
		assertEquals("bar", context.getBean("test-job", Job.class).getName());
		assertEquals(4, context.getBean("foo", Foo.class).values[1], 0.01);
	}

	@SuppressWarnings("resource")
	@Test
	public void testBeanFactoryPostProcessorOrderRespected() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(
				new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(), "placeholder-context.xml")));
		factory.setApplicationContext(new ClassPathXmlApplicationContext(ClassUtils.addResourcePathToPackagePath(
				getClass(), "parent-context.xml")));
		ConfigurableApplicationContext context = factory.createApplicationContext();
		assertEquals("test-job", context.getBeanNamesForType(Job.class)[0]);
		assertEquals("spam", context.getBean("test-job", Job.class).getName());
	}

	@Test
	public void testBeanFactoryProfileRespected() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(
				new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(), "profiles.xml")));
		@SuppressWarnings("resource")
		ClassPathXmlApplicationContext parentContext = new ClassPathXmlApplicationContext(ClassUtils.addResourcePathToPackagePath(
				getClass(), "parent-context.xml"));
		parentContext.getEnvironment().setActiveProfiles("preferred");
		factory.setApplicationContext(parentContext);
		@SuppressWarnings("resource")
		ConfigurableApplicationContext context = factory.createApplicationContext();
		assertEquals("test-job", context.getBeanNamesForType(Job.class)[0]);
		assertEquals("spam", context.getBean("test-job", Job.class).getName());
	}

	@SuppressWarnings("resource")
	@Test
	public void testBeanFactoryPostProcessorsNotCopied() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(
				new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(), "child-context.xml")));
		factory.setApplicationContext(new ClassPathXmlApplicationContext(ClassUtils.addResourcePathToPackagePath(
				getClass(), "parent-context.xml")));
		@SuppressWarnings("unchecked")
		Class<? extends BeanFactoryPostProcessor>[] classes = (Class<? extends BeanFactoryPostProcessor>[]) new Class<?>[0];
		factory.setBeanFactoryPostProcessorClasses(classes);
		ConfigurableApplicationContext context = factory.createApplicationContext();
		assertEquals("test-job", context.getBeanNamesForType(Job.class)[0]);
		assertEquals("${foo}", context.getBean("test-job", Job.class).getName());
		assertEquals(4, context.getBean("foo", Foo.class).values[1], 0.01);
	}

	@SuppressWarnings("resource")
	@Test
	public void testBeanFactoryConfigurationNotCopied() {
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(),
				"child-context.xml")));
		factory.setApplicationContext(new ClassPathXmlApplicationContext(ClassUtils.addResourcePathToPackagePath(
				getClass(), "parent-context.xml")));
		factory.setCopyConfiguration(false);
		ConfigurableApplicationContext context = factory.createApplicationContext();
		assertEquals("test-job", context.getBeanNamesForType(Job.class)[0]);
		assertEquals("bar", context.getBean("test-job", Job.class).getName());
		// The CustomEditorConfigurer is a BeanFactoryPostProcessor so the
		// editor gets copied anyway!
		assertEquals(4, context.getBean("foo", Foo.class).values[1], 0.01);
	}

	@Test
	public void testEquals() throws Exception {
		Resource resource = new ClassPathResource(ClassUtils.addResourcePathToPackagePath(getClass(),
				"child-context.xml"));
		GenericApplicationContextFactory factory = new GenericApplicationContextFactory(resource);
		GenericApplicationContextFactory other = new GenericApplicationContextFactory(resource);
		assertEquals(other, factory);
		assertEquals(other.hashCode(), factory.hashCode());
	}

	public static class Foo {
		private double[] values;

		public void setValues(double[] values) {
			this.values = values;
		}
	}

}
