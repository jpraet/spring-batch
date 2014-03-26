package org.springframework.batch.item.xml;

import javax.xml.bind.JAXBElement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.MasterDetailItem;
import org.springframework.batch.item.jaxb.DetailType;
import org.springframework.batch.item.jaxb.MasterType;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

public class MasterDetailStaxItemReaderTest {

	private MasterDetailStaxItemReader<JAXBElement<MasterType>, JAXBElement<DetailType>> reader;

	private Jaxb2Marshaller marshaller;

	private ExecutionContext executionContext = new ExecutionContext();

	@Before
	public void setUp() throws Exception {
		initReader();
	}

	private void initReader() throws Exception {
		reader = new MasterDetailStaxItemReader<JAXBElement<MasterType>, JAXBElement<DetailType>>();
		reader.setMasterFragmentRootElementName("master");
		reader.setDetailFragmentRootElementName("detail");
		reader.setDetailFragmentGroupName("details");
		reader.setResource(new ClassPathResource("/org/springframework/batch/item/master-detail/master-detail.xml"));
		marshaller = new Jaxb2Marshaller();
		marshaller.setContextPath("org.springframework.batch.item.jaxb");
		marshaller.afterPropertiesSet();
		reader.setUnmarshaller(marshaller);
	}

	@Test
	public void testReadSuccess() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testReadSuccessSingleLine() throws Exception {
		reader.setResource(new ClassPathResource("/org/springframework/batch/item/master-detail/master-detail-single-line.xml"));
		reader.afterPropertiesSet();
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testReadSuccessWithCustomItemClass() throws Exception {
		reader.setItemClass(CustomItemClass.class);
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testResumeAfterM1D1() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		reader.update(executionContext);

		reader.close();
		initReader();
		reader.open(executionContext);

		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testResumeAfterM1D2() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		reader.update(executionContext);

		reader.close();
		initReader();
		reader.open(executionContext);

		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testResumeAfterM2D1() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		reader.update(executionContext);

		reader.close();
		initReader();
		reader.open(executionContext);

		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testResumeAfterM3() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		reader.update(executionContext);

		reader.close();
		initReader();
		reader.open(executionContext);

		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testResumeAfterM4D3() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		reader.update(executionContext);

		reader.close();
		initReader();
		reader.open(executionContext);

		item = reader.read();
		Assert.assertNull(item);
	}

	@Test
	public void testResumeAfterEOF() throws Exception {
		reader.open(executionContext);
		MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item;
		item = reader.read();
		assertItem(item, 1, 1);
		item = reader.read();
		assertItem(item, 1, 2);
		item = reader.read();
		assertItem(item, 2, 1);
		item = reader.read();
		assertItem(item, 3, 0);
		item = reader.read();
		assertItem(item, 4, 1);
		item = reader.read();
		assertItem(item, 4, 2);
		item = reader.read();
		assertItem(item, 4, 3);
		item = reader.read();
		Assert.assertNull(item);
		reader.update(executionContext);

		reader.close();
		initReader();
		reader.open(executionContext);

		item = reader.read();
		Assert.assertNull(item);
	}
	
	private void assertItem(MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> item, int masterId,
			int detailId) {
		MasterType master = item.getMaster().getValue();
		DetailType detail = null;
		if (item.getDetail() != null) {
			detail = item.getDetail().getValue();
		}
		Assert.assertEquals("master", masterId, item.getMasterCount());
		Assert.assertEquals("master-" + masterId, master.getValue());
		if (detailId > 0) {
			Assert.assertEquals("detail", detailId, item.getDetailCount());
			Assert.assertEquals("detail-" + masterId + "." + detailId, detail.getValue());
		} else {
			Assert.assertNull(detail);
		}
	}

	public static final class CustomItemClass extends
			MasterDetailItem<JAXBElement<MasterType>, JAXBElement<DetailType>> {
	}

}
