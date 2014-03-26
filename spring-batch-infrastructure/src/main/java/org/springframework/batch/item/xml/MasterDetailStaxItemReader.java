package org.springframework.batch.item.xml;

import java.io.InputStream;
import java.util.NoSuchElementException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.MasterDetailItem;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.xml.stax.FragmentEventReader;
import org.springframework.batch.item.xml.stax.MasterDetailFragmentEventReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.oxm.Unmarshaller;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * ItemReader for reading XML documents with a master/detail record structure.
 * 
 * TODO: this class contains a lot of copy/paste from StaxEventItemReader because StaxEventItemReader was not designed
 * with extensible inheritance in mind.
 * 
 * @param <M> the master record
 * @param <D> the detail record
 */
public class MasterDetailStaxItemReader<M, D> extends
		AbstractItemCountingItemStreamItemReader<MasterDetailItem<M, D>> implements
		ResourceAwareItemReaderItemStream<MasterDetailItem<M, D>>, InitializingBean {

	private static final Log LOG = LogFactory.getLog(MasterDetailStaxItemReader.class);

	private static final String MASTER_COUNT = "master.count";

	private static final String DETAIL_COUNT = "detail.count";

	private int currentMasterCount = 0;

	private int currentDetailCount = 0;

	private FragmentEventReader fragmentReader;

	private XMLEventReader eventReader;

	// TODO: do we need seperate unmarshallers for master and detail items?
	private Unmarshaller unmarshaller;

	private Resource resource;

	private InputStream inputStream;

	private boolean noInput;

	private boolean strict = true;

	private String masterFragmentRootElementName;

	private String masterFragmentRootElementNameSpace;

	private String detailFragmentRootElementName;

	private String detailFragmentRootElementNameSpace;

	private String detailFragmentGroupName;

	private String detailFragmentGroupNameSpace;

	private M currentMasterItem;

	private Class<? extends MasterDetailItem> itemClass = MasterDetailItem.class;

	/**
	 * MasterDetailStaxItemReader constructor.
	 */
	public MasterDetailStaxItemReader() {
		setName(ClassUtils.getShortName(MasterDetailStaxItemReader.class));
	}

	/**
	 * In strict mode the reader will throw an exception on
	 * {@link #open(org.springframework.batch.item.ExecutionContext)} if the
	 * input resource does not exist.
	 * 
	 * @param strict
	 *            false by default
	 */
	public void setStrict(boolean strict) {
		this.strict = strict;
	}

	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	/**
	 * @param unmarshaller
	 *            maps xml fragments corresponding to records to objects
	 */
	public void setUnmarshaller(Unmarshaller unmarshaller) {
		this.unmarshaller = unmarshaller;
	}

	/**
	 * @param masterFragmentRootElementName
	 *            name of the root element of the master item fragment
	 */
	public void setMasterFragmentRootElementName(String masterFragmentRootElementName) {
		this.masterFragmentRootElementName = masterFragmentRootElementName;
	}

	/**
	 * @param detailFragmentRootElementName
	 *            name of the root element of the detail item fragment
	 */
	public void setDetailFragmentRootElementName(String detailFragmentRootElementName) {
		this.detailFragmentRootElementName = detailFragmentRootElementName;
	}

	/**
	 * @param detailFragmentGroupName
	 *            name of the detail fragment group (optional)
	 */
	public void setDetailFragmentGroupName(String detailFragmentGroupName) {
		this.detailFragmentGroupName = detailFragmentGroupName;
	}

	/**
	 * @param itemClass the item class (defaults to MasterDetailItem.class)
	 */
	public void setItemClass(Class<? extends MasterDetailItem> itemClass) {
		this.itemClass = itemClass;
	}

	/**
	 * Ensure that all required dependencies for the ItemReader to run are
	 * provided after all properties have been set.
	 * 
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 * @throws Exception if a required dependency was not provided
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(unmarshaller, "Missing required property 'unmarshaller'");
		Assert.hasLength(masterFragmentRootElementName, "Missing required property 'masterFragmentRootElementName'");
		if (masterFragmentRootElementName.contains("{")) {
			masterFragmentRootElementNameSpace = masterFragmentRootElementName.replaceAll("\\{(.*)\\}.*", "$1");
			masterFragmentRootElementName = masterFragmentRootElementName.replaceAll("\\{.*\\}(.*)", "$1");
		}
		Assert.hasLength(detailFragmentRootElementName, "Missing required property 'detailFragmentRootElementName'");
		if (detailFragmentRootElementName.contains("{")) {
			detailFragmentRootElementNameSpace = detailFragmentRootElementName.replaceAll("\\{(.*)\\}.*", "$1");
			detailFragmentRootElementName = detailFragmentRootElementName.replaceAll("\\{.*\\}(.*)", "$1");
		}
		if (StringUtils.hasLength(detailFragmentGroupName)) {
			if (detailFragmentGroupName.contains("{")) {
				detailFragmentGroupNameSpace = detailFragmentGroupName.replaceAll("\\{(.*)\\}.*", "$1");
				detailFragmentGroupName = detailFragmentGroupName.replaceAll("\\{.*\\}(.*)", "$1");
			}
		}
	}

	@Override
	protected void doClose() throws Exception {
		try {
			if (fragmentReader != null) {
				fragmentReader.close();
			}
			if (inputStream != null) {
				inputStream.close();
			}
		} finally {
			fragmentReader = null;
			inputStream = null;
		}

	}

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (executionContext.containsKey(getExecutionContextKey(MASTER_COUNT))) {
			currentMasterCount = executionContext.getInt(getExecutionContextKey(MASTER_COUNT));
		}
		if (executionContext.containsKey(getExecutionContextKey(DETAIL_COUNT))) {
			currentDetailCount = executionContext.getInt(getExecutionContextKey(DETAIL_COUNT));
		}
		// need to initialize the state before calling super.open() because the
		// latter will call jumpToItem()
		super.open(executionContext);
	}

	@Override
	protected void doOpen() throws Exception {
		Assert.notNull(resource, "The Resource must not be null.");

		noInput = true;
		if (!resource.exists()) {
			if (strict) {
				throw new IllegalStateException("Input resource must exist (reader is in 'strict' mode)");
			}
			LOG.warn("Input resource does not exist " + resource.getDescription());
			return;
		}
		if (!resource.isReadable()) {
			if (strict) {
				throw new IllegalStateException("Input resource must be readable (reader is in 'strict' mode)");
			}
			LOG.warn("Input resource is not readable " + resource.getDescription());
			return;
		}

		inputStream = resource.getInputStream();
		eventReader = XMLInputFactory.newInstance().createXMLEventReader(inputStream);
		QName detailFragmentGroupQName = null;
		if (StringUtils.hasLength(detailFragmentGroupName)) {
			detailFragmentGroupQName = new QName(detailFragmentGroupNameSpace, detailFragmentGroupName);
		}
		fragmentReader = new MasterDetailFragmentEventReader(eventReader, new QName(masterFragmentRootElementNameSpace,
				masterFragmentRootElementName), new QName(detailFragmentRootElementNameSpace,
						detailFragmentRootElementName), detailFragmentGroupQName);
		noInput = false;
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		super.update(executionContext);
		if (isSaveState()) {
			Assert.notNull(executionContext, "ExecutionContext must not be null");
			executionContext.putInt(getExecutionContextKey(MASTER_COUNT), currentMasterCount);
			executionContext.putInt(getExecutionContextKey(DETAIL_COUNT), currentDetailCount);
		}
	}

	/**
	 * Move to next fragment and map it to item.
	 */
	@Override
	protected MasterDetailItem<M, D> doRead() throws Exception {
		if (noInput) {
			return null;
		}

		MasterDetailItem<M, D> item = null;

		boolean success = false;
		try {
			success = moveCursorToNextFragment(fragmentReader);
		} catch (NonTransientResourceException e) {
			// Prevent caller from retrying indefinitely since this is fatal
			noInput = true;
			throw e;
		}
		if (success) {
			item = newMasterDetailItem();
			StartElement startElement = fragmentReader.peek().asStartElement();
			if (masterFragmentRootElementName.equals(startElement.getName().getLocalPart())) {
				readMasterFragment(item);
				try {
					success = moveCursorToNextFragment(fragmentReader);
				} catch (NonTransientResourceException e) {
					// Prevent caller from retrying indefinitely since this is
					// fatal
					noInput = true;
					throw e;
				}
				if (success) {
					startElement = fragmentReader.peek().asStartElement();
					if (detailFragmentRootElementName.equals(startElement.getName().getLocalPart())) {
						readDetailFragment(item);
					}
				}
			} else if (detailFragmentRootElementName.equals(startElement.getName().getLocalPart())) {
				readDetailFragment(item);
			}
		}

		return item;
	}

	@SuppressWarnings("unchecked")
	private MasterDetailItem<M, D> newMasterDetailItem() throws IllegalAccessException, InstantiationException {
		return itemClass.newInstance();
	}

	private void readMasterFragment(MasterDetailItem<M, D> item) throws Exception {
		@SuppressWarnings("unchecked")
		M masterFragment = (M) unmarshalFragment();
		currentMasterItem = masterFragment;
		currentDetailCount = 0;
		currentMasterCount++;
		item.setMaster(masterFragment);
		item.setMasterCount(currentMasterCount);
	}

	private void readDetailFragment(MasterDetailItem<M, D> item) throws Exception {
		@SuppressWarnings("unchecked")
		D detailFragment = (D) unmarshalFragment();
		item.setMaster(currentMasterItem);
		item.setMasterCount(currentMasterCount);
		currentDetailCount++;
		item.setDetail(detailFragment);
		item.setDetailCount(currentDetailCount);
	}

	private Object unmarshalFragment() throws Exception {
		fragmentReader.markStartFragment();
		try {
			return unmarshaller.unmarshal(StaxUtils.getSource(fragmentReader));
		} finally {
			fragmentReader.markFragmentProcessed();
		}
	}

	/**
	 * Responsible for moving the cursor before the StartElement of the next
	 * master or detail fragment root.
	 * 
	 * @return <code>true</code> if next fragment was found, <code>false</code>
	 *         otherwise.
	 * @throws NonTransientResourceException
	 *             if the cursor could not be moved. This will be treated as
	 *             fatal and subsequent calls to read will return null.
	 */
	protected boolean moveCursorToNextFragment(XMLEventReader reader) throws NonTransientResourceException {
		try {
			while (true) {
				while (reader.peek() != null && !reader.peek().isStartElement()) {
					reader.nextEvent();
				}
				if (reader.peek() == null) {
					return false;
				}
				QName startElementName = ((StartElement) reader.peek()).getName();
				if (startElementName.getLocalPart().equals(masterFragmentRootElementName)) {
					if (masterFragmentRootElementNameSpace == null
							|| startElementName.getNamespaceURI().equals(masterFragmentRootElementNameSpace)) {
						return true;
					}
				} else if (startElementName.getLocalPart().equals(detailFragmentRootElementName)) {
					if (detailFragmentRootElementNameSpace == null
							|| startElementName.getNamespaceURI().equals(detailFragmentRootElementNameSpace)) {
						return true;
					}
				}
				reader.nextEvent();

			}
		} catch (XMLStreamException e) {
			throw new NonTransientResourceException("Error while reading from event reader", e);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void jumpToItem(int itemIndex) throws Exception {
		// we use the currentMasterCount and currentDetailCount instead of itemIndex to jump to the item
		for (int i = 0; i < currentMasterCount - 1; i++) {
			try {
				readToStartMasterFragment();
				readToEndMasterFragment();
			} catch (NoSuchElementException e) {
				if (currentMasterCount == (i + 1)) {
					// we can presume a NoSuchElementException on the last item
					// means the EOF was reached on the last run
					return;
				} else {
					// if NoSuchElementException occurs on an item other than
					// the last one, this indicates a problem
					throw e;
				}
			}
		}		
		// read current master item for restart
		moveCursorToNextFragment(eventReader);
		currentMasterItem = (M) unmarshalFragment();		
		for (int i = 0; i < currentDetailCount; i++) {
			try {
				readToStartDetailFragment();
				readToEndDetailFragment();
			} catch (NoSuchElementException e) {
				if (currentDetailCount == (i + 1)) {
					// we can presume a NoSuchElementException on the last item
					// means the EOF was reached on the last run
					return;
				} else {
					// if NoSuchElementException occurs on an item other than
					// the last one, this indicates a problem
					throw e;
				}
			}
		}
	}

	private void readToStartMasterFragment() throws XMLStreamException {
		while (true) {
			XMLEvent nextEvent = eventReader.nextEvent();
			if (nextEvent.isStartElement()
					&& ((StartElement) nextEvent).getName().getLocalPart().equals(masterFragmentRootElementName)) {
				return;
			}			
		}
	}

	private void readToEndMasterFragment() throws XMLStreamException {
		while (true) {
			XMLEvent nextEvent = eventReader.nextEvent();
			if (nextEvent.isEndElement()
					&& ((EndElement) nextEvent).getName().getLocalPart().equals(masterFragmentRootElementName)) {
				return;
			}			
		}
	}

	private void readToStartDetailFragment() throws XMLStreamException {
		while (true) {
			XMLEvent nextEvent = eventReader.nextEvent();
			if (nextEvent.isStartElement()
					&& ((StartElement) nextEvent).getName().getLocalPart().equals(detailFragmentRootElementName)) {
				return;
			}
		}
	}

	private void readToEndDetailFragment() throws XMLStreamException {
		while (true) {
			XMLEvent nextEvent = eventReader.nextEvent();
			if (nextEvent.isEndElement()
					&& ((EndElement) nextEvent).getName().getLocalPart().equals(detailFragmentRootElementName)) {
				return;
			}
		}
	}

}
