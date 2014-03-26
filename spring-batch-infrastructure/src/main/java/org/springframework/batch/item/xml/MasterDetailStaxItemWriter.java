package org.springframework.batch.item.xml;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.MasterDetailItem;
import org.springframework.batch.item.xml.stax.MasterDetailFragmentEventWriter;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.XmlMappingException;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * ItemWriter for writing XML documents with a master/detail record structure.
 * 
 * @param <T> the MasterDetailItem
 */
public class MasterDetailStaxItemWriter<T extends MasterDetailItem<?, ?>> extends StaxEventItemWriter<T> {

	// this overwrites the record.count of StaxEventItemWriter
	private static final String WRITE_STATISTICS_NAME = "record.count";

	private static final String MASTER_COUNT = "master.count";

	private static final String MASTER_RECORD_OPENED = "master.record.opened";

	private long currentMasterCount = 0;

	private long currentRecordCount = 0;

	private boolean masterRecordOpened = false;

	private MasterDetailFragmentEventWriter masterDetailFragmentEventWriter;

	private Writer writer;

	private Marshaller marshaller;

	private String masterFragmentRootElementName;

	private String masterFragmentGroupName;

	private String detailFragmentGroupName;

	@Override
	public void write(List<? extends T> items) throws XmlMappingException, Exception {
		if (currentRecordCount == 0 && hasMasterFragmentGroup()) {
			openElement(masterDetailFragmentEventWriter, masterFragmentGroupName);
		}
		currentRecordCount += items.size();
		for (MasterDetailItem<?, ?> item : items) {
			if (item.getMasterCount() == currentMasterCount) {
				// append detail record to current master record
				marshaller.marshal(item.getDetail(), createStaxResult());
			} else {
				currentMasterCount = item.getMasterCount();
				// write new master record
				if (masterRecordOpened) {
					closeCurrentMasterRecord();
				}
				if (item.getDetail() == null) {
					// write master record without detail records
					marshaller.marshal(item.getMaster(), createStaxResult());
					masterRecordOpened = false;
				} else {
					// write master record with detail record
					masterDetailFragmentEventWriter.startWritingPartialMasterFragment();
					marshaller.marshal(item.getMaster(), createStaxResult());
					masterRecordOpened = true;
					masterDetailFragmentEventWriter.stopWritingPartialMasterFragment();
					if (hasDetailFragmentGroup()) {
						openElement(masterDetailFragmentEventWriter, detailFragmentGroupName);
					}
					marshaller.marshal(item.getDetail(), createStaxResult());
				}
			}
		}
		// call super.write() with empty list to invoke flush and forceSync logic
		super.write(Collections.<T> emptyList());
	}

	private void openElement(XMLEventWriter xmlEventWriter, String element) throws XMLStreamException,
			FactoryConfigurationError {
		String tagName = element;
		String tagNameSpacePrefix = "";
		String tagNameSpace = null;
		if (tagName.contains("{")) {
			tagNameSpace = tagName.replaceAll("\\{(.*)\\}.*", "$1");
			tagName = tagName.replaceAll("\\{.*\\}(.*)", "$1");
			if (tagName.contains(":")) {
				tagNameSpacePrefix = tagName.replaceAll("(.*):.*", "$1");
				tagName = tagName.replaceAll(".*:(.*)", "$1");
			}
		}
		XMLEventFactory xmlEventFactory = createXmlEventFactory();
		xmlEventWriter.add(xmlEventFactory.createStartElement(tagNameSpacePrefix, tagNameSpace, tagName));
	}

	private void closeElement(XMLEventWriter xmlEventWriter, String element)
			throws XMLStreamException, FactoryConfigurationError {
		String tagName = element;
		String tagNameSpacePrefix = "";
		if (tagName.contains("{")) {
			tagName = tagName.replaceAll("\\{.*\\}(.*)", "$1");
			if (tagName.contains(":")) {
				tagNameSpacePrefix = tagName.replaceAll("(.*):.*", "$1") + ":";
				tagName = tagName.replaceAll(".*:(.*)", "$1");
			}
		}
		try {
			writer.write("</" + tagNameSpacePrefix + tagName + ">");
		} catch (IOException ioe) {
			throw new DataAccessResourceFailureException("Unable to close element: [" + element + "]", ioe);
		}
	}

	private void closeCurrentMasterRecord() {
		try {
			if (hasDetailFragmentGroup()) {
				closeElement(masterDetailFragmentEventWriter, detailFragmentGroupName);
			}
			closeElement(masterDetailFragmentEventWriter, masterFragmentRootElementName);
		} catch (XMLStreamException e) {
			throw new ItemStreamException("Failed to write close tag for element: "
					+ masterFragmentRootElementName, e);
		} catch (FactoryConfigurationError e) {
			throw new ItemStreamException("Failed to write close tag for element: "
					+ masterFragmentRootElementName, e);
		}
	}

	@Override
	protected XMLEventWriter createXmlEventWriter(XMLOutputFactory outputFactory, Writer writer)
			throws XMLStreamException {
		XMLEventWriter delegate = super.createXmlEventWriter(outputFactory, writer);
		this.writer = writer;
		this.masterDetailFragmentEventWriter = new MasterDetailFragmentEventWriter(delegate);
		return masterDetailFragmentEventWriter;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		if (executionContext.containsKey(getExecutionContextKey(MASTER_COUNT))) {
			currentMasterCount = executionContext.getLong(getExecutionContextKey(MASTER_COUNT));
		}
		if (executionContext.containsKey(getExecutionContextKey(WRITE_STATISTICS_NAME))) {
			currentRecordCount = executionContext.getLong(getExecutionContextKey(WRITE_STATISTICS_NAME));
		}
		if (executionContext.containsKey(getExecutionContextKey(MASTER_RECORD_OPENED))) {
			masterRecordOpened =
					Boolean.valueOf(executionContext.getString(getExecutionContextKey(MASTER_RECORD_OPENED)));
		}
	}

	@Override
	public void update(ExecutionContext executionContext) {
		super.update(executionContext);
		executionContext.putLong(getExecutionContextKey(MASTER_COUNT), currentMasterCount);
		executionContext.putLong(getExecutionContextKey(WRITE_STATISTICS_NAME), currentRecordCount);
		executionContext.putString(getExecutionContextKey(MASTER_RECORD_OPENED), String.valueOf(masterRecordOpened));
	}

	@Override
	public void close() {
		try {
			if (masterRecordOpened) {
				closeCurrentMasterRecord();
			}
			if (hasMasterFragmentGroup()) {
				closeElement(masterDetailFragmentEventWriter, masterFragmentGroupName);
			}
		} catch (XMLStreamException e) {
			throw new ItemStreamException("Failed to write close tags", e);
		} catch (FactoryConfigurationError e) {
			throw new ItemStreamException("Failed to write close tags", e);
		}
		super.close();
	}

	private boolean hasMasterFragmentGroup() {
		return StringUtils.hasText(masterFragmentGroupName);
	}

	private boolean hasDetailFragmentGroup() {
		return StringUtils.hasText(detailFragmentGroupName);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		super.afterPropertiesSet();
		Assert.notNull(masterFragmentRootElementName, "Missing required property 'masterFragmentRootElementName'");
	}

	@Override
	public void setMarshaller(Marshaller marshaller) {
		super.setMarshaller(marshaller);
		this.marshaller = marshaller;
	}

	public void setMasterFragmentRootElementName(String masterFragmentRootElementName) {
		this.masterFragmentRootElementName = masterFragmentRootElementName;
	}

	public void setMasterFragmentGroupName(String masterFragmentGroupName) {
		this.masterFragmentGroupName = masterFragmentGroupName;
	}

	public void setDetailFragmentGroupName(String detailFragmentGroupName) {
		this.detailFragmentGroupName = detailFragmentGroupName;
	}

}