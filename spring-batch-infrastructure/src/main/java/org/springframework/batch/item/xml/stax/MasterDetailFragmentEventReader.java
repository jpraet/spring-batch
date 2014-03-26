package org.springframework.batch.item.xml.stax;

import java.util.LinkedList;
import java.util.NoSuchElementException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

import org.springframework.batch.item.xml.stax.DefaultFragmentEventReader;
import org.springframework.batch.item.xml.stax.FragmentEventReader;
import org.springframework.dao.DataAccessResourceFailureException;

/**
 * FragmentEventReader implementation used in {@link MasterDetailStaxItemReader}.
 * 
 * This class extends the {@link DefaultFragmentEventReader} with the functionality to
 * simulate the end of the master record, when a detail record fragment (or -group) is encountered
 * while parsing the master record.
 */
public class MasterDetailFragmentEventReader implements FragmentEventReader {

	private XMLEventReader wrappedEventReader;

	private QName masterFragmentQName;

	private QName detailFragmentQName;

	private QName detailFragmentGroupQName;

	private boolean readingMasterFragment = false;

	private boolean simulateCloseMasterFragment = false;

	private boolean simulateEndDocument = false;

	private LinkedList<QName> openedMasterTags = new LinkedList<QName>();

	/**
	 * MasterDetailFragmentEventReader constructor.
	 * 
	 * @param wrappedEventReader the wrapped XMLEventReader
	 * @param masterFragmentQName the QName of the master fragment
	 * @param detailFragmentQName the QName of the detail fragment
	 * @param detailFragmentGroupQName the QName of the detail fragment group (optional)
	 */
	public MasterDetailFragmentEventReader(XMLEventReader wrappedEventReader, QName masterFragmentQName,
			QName detailFragmentQName, QName detailFragmentGroupQName) {
		this.wrappedEventReader = wrappedEventReader;
		this.masterFragmentQName = masterFragmentQName;
		this.detailFragmentQName = detailFragmentQName;
		this.detailFragmentGroupQName = detailFragmentGroupQName;
	}

	@Override
	public void markStartFragment() {
		try {
			QName fragmentQName = peek().asStartElement().getName();
			if (fragmentQName.getLocalPart().equals(masterFragmentQName.getLocalPart())) {
				openedMasterTags.clear();
				readingMasterFragment = true;
			}
		} catch (XMLStreamException e) {
			throw new DataAccessResourceFailureException("Error reading XML stream", e);
		}
	}

	@Override
	public XMLEvent nextEvent() throws XMLStreamException {
		if (simulateEndDocument) {
			throw new NoSuchElementException();
		}
		XMLEvent event = wrappedEventReader.peek();
		XMLEvent proxyEvent = alterEvent(event, false);
		if (event == proxyEvent) {
			wrappedEventReader.nextEvent();
		}
		return proxyEvent;
	}

	@Override
	public XMLEvent peek() throws XMLStreamException {
		if (simulateEndDocument) {
			return null;
		}
		return alterEvent(wrappedEventReader.peek(), true);
	}

	private XMLEvent alterEvent(XMLEvent event, boolean peek) {
		if (!peek) {
			if (readingMasterFragment && event != null && event.isStartElement()) {
				String elementName = event.asStartElement().getName().getLocalPart();
				if (elementName.equals(detailFragmentQName.getLocalPart())
						|| (detailFragmentGroupQName != null && elementName
								.equals(detailFragmentGroupQName.getLocalPart()))) {
					simulateCloseMasterFragment = true;
				} else {
					openedMasterTags.add(event.asStartElement().getName());
				}
			} else if (readingMasterFragment && event != null && event.isEndElement()) {
				openedMasterTags.removeLast();
			}
		}
		if (simulateCloseMasterFragment) {
			if (openedMasterTags.isEmpty()) {
				if (!peek) {
					simulateEndDocument = true;
				}
				return XMLEventFactory.newInstance().createEndDocument();
			} else {
				return XMLEventFactory.newInstance().createEndElement(
						peek ? openedMasterTags.getLast() : openedMasterTags.removeLast(),
						null);
			}
		} else {
			return event;
		}
	}

	@Override
	public void markFragmentProcessed() {
		reset();
	}

	@Override
	public void reset() {
		this.openedMasterTags.clear();
		this.readingMasterFragment = false;
		this.simulateCloseMasterFragment = false;
		this.simulateEndDocument = false;
	}

	@Override
	public void close() throws XMLStreamException {
		wrappedEventReader.close();
	}

	@Override
	public String getElementText() throws XMLStreamException {
		return wrappedEventReader.getElementText();
	}

	@Override
	public Object getProperty(String name) throws IllegalArgumentException {
		return wrappedEventReader.getProperty(name);
	}

	@Override
	public boolean hasNext() {
		try {
			if (peek() != null) {
				return true;
			}
		} catch (XMLStreamException e) {
			throw new DataAccessResourceFailureException("Error reading XML stream", e);
		}
		return false;
	}

	@Override
	public XMLEvent nextTag() throws XMLStreamException {
		return wrappedEventReader.nextTag();
	}

	@Override
	public Object next() {
		try {
			return nextEvent();
		} catch (XMLStreamException e) {
			throw new DataAccessResourceFailureException("Error reading XML stream", e);
		}
	}

	@Override
	public void remove() {
		wrappedEventReader.remove();
	}

}