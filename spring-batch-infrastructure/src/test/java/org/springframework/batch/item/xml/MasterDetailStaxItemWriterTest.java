package org.springframework.batch.item.xml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.MasterDetailItem;
import org.springframework.batch.item.jaxb.DetailType;
import org.springframework.batch.item.jaxb.MasterType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

public class MasterDetailStaxItemWriterTest {

	private MasterDetailStaxItemWriter<MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>>> writer;

	private List<MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>>> items;

	private Jaxb2Marshaller marshaller;

	private ExecutionContext executionContext = new ExecutionContext();

	@Before
	public void setUp() throws Exception {
		initMarshaller();
		initItems();
		initWriter();
	}

	private void initMarshaller() throws Exception {
		marshaller = new Jaxb2Marshaller();
		marshaller.setContextPath("org.springframework.batch.item.jaxb");
		marshaller.afterPropertiesSet();
	}

	private void initItems() throws Exception {
		items = new ArrayList<MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>>>();
		MasterDetailStaxItemReader<JAXBElement<MasterType>, JAXBElement<DetailType>> reader = new MasterDetailStaxItemReader<JAXBElement<MasterType>, JAXBElement<DetailType>>();
		reader.setMasterFragmentRootElementName("master");
		reader.setDetailFragmentRootElementName("detail");
		reader.setDetailFragmentGroupName("details");
		reader.setResource(new ClassPathResource("/org/springframework/batch/item/master-detail/master-detail.xml"));
		reader.setUnmarshaller(marshaller);
		reader.afterPropertiesSet();
		reader.open(new ExecutionContext());
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		while ((item = reader.read()) != null) {
			items.add(item);
		}
	}

	private void initWriter() throws Exception {
		writer = new MasterDetailStaxItemWriter<MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>>>();
		writer.setMarshaller(marshaller);
		writer.setRootTagName("{http://www.springframework.org/test}tns:root");
		writer.setMasterFragmentRootElementName("{http://www.springframework.org/test}tns:master");
		writer.setMasterFragmentGroupName("{http://www.springframework.org/test}tns:masters");
		writer.setDetailFragmentGroupName("{http://www.springframework.org/test}tns:details");
		writer.setResource(new FileSystemResource("target/master-detail.xml"));
		writer.setHeaderCallback(new StaxWriterCallback() {
			@Override
			public void write(XMLEventWriter writer) throws IOException {
				XMLEventFactory xmlEventFactory = XMLEventFactory.newInstance();
				try {
					writer.add(xmlEventFactory.createCharacters(IOUtils.LINE_SEPARATOR_UNIX + "\t"));
					writer.add(xmlEventFactory.createStartElement("tns", "http://www.springframework.org/test",
							"header"));
					writer.add(xmlEventFactory.createCharacters("header"));
					writer.add(xmlEventFactory.createEndElement("tns", "http://www.springframework.org/test", "header"));
				} catch (XMLStreamException e) {
					throw new RuntimeException(e);
				}
			}
		});
		writer.afterPropertiesSet();
	}

	@Test
	public void testWriteSuccess() throws Exception {
		writer.open(executionContext);
		writer.write(items);
		writer.update(executionContext);
		writer.close();

		assertOutput();
	}

	@Test
	public void testResumeAfterM1D1() throws Exception {
		writer.open(executionContext);
		writer.write(items.subList(0, 1));
		writer.update(executionContext);
		writer.close();

		initWriter();
		writer.open(executionContext);
		writer.write(items.subList(1, items.size()));
		writer.update(executionContext);
		writer.close();

		assertOutput();
	}

	@Test
	public void testResumeAfterM1D2() throws Exception {
		writer.open(executionContext);
		writer.write(items.subList(0, 2));
		writer.update(executionContext);
		writer.close();

		initWriter();
		writer.open(executionContext);
		writer.write(items.subList(2, items.size()));
		writer.update(executionContext);
		writer.close();

		assertOutput();
	}

	@Test
	public void testResumeAfterM2D1() throws Exception {
		writer.open(executionContext);
		writer.write(items.subList(0, 3));
		writer.update(executionContext);
		writer.close();

		initWriter();
		writer.open(executionContext);
		writer.write(items.subList(3, items.size()));
		writer.update(executionContext);
		writer.close();

		assertOutput();
	}

	@Test
	public void testResumeAfterM3() throws Exception {
		writer.open(executionContext);
		writer.write(items.subList(0, 4));
		writer.update(executionContext);
		writer.close();

		initWriter();
		writer.open(executionContext);
		writer.write(items.subList(4, items.size()));
		writer.update(executionContext);
		writer.close();

		assertOutput();

	}

	@Test
	public void testResumeAfterM4D3() throws Exception {
		writer.open(executionContext);
		writer.write(items.subList(0, items.size() - 1));
		writer.update(executionContext);
		writer.close();

		initWriter();
		writer.open(executionContext);
		writer.write(items.subList(items.size() - 1, items.size()));
		writer.update(executionContext);
		writer.close();

		assertOutput();
	}

	@Test
	public void testResumeAfterEOF() throws Exception {
		writer.open(executionContext);
		writer.write(items);
		writer.update(executionContext);
		writer.close();

		initWriter();
		writer.open(executionContext);
		writer.write(Collections.<MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>>> emptyList());
		writer.update(executionContext);
		writer.close();

		assertOutput();
	}

	private void assertOutput() throws Exception {
		String expected = FileUtils.readFileToString(
				new ClassPathResource("/org/springframework/batch/item/master-detail/master-detail-single-line.xml")
						.getFile()).replaceAll("\\s", "").replaceFirst("<\\?.*>", "");
		String actual = FileUtils.readFileToString(new FileSystemResource("target/master-detail.xml").getFile())
				.replaceAll("\\s", "").replaceFirst("<\\?.*>", "");
		Assert.assertEquals(expected, actual);
	}
}
