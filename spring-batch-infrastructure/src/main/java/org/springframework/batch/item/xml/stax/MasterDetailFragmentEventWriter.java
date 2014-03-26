package org.springframework.batch.item.xml.stax;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;

/**
 * XMLEventWriter wrapper used by MasterDetailStaxItemWriter.
 */
public class MasterDetailFragmentEventWriter extends AbstractEventWriterWrapper {

	private boolean writingPartialMasterFragment;

	private QName masterFragmentQName;

	/**
	 * MasterDetailFragmentEventWriter constructor.
	 * 
	 * @param xmlEventWriter the wrapped XMLEventWriter
	 */
	public MasterDetailFragmentEventWriter(XMLEventWriter xmlEventWriter) {
		super(xmlEventWriter);
	}

	/**
	 * Enabling partial martial fragment write mode.
	 */
	public void startWritingPartialMasterFragment() {
		writingPartialMasterFragment = true;
		masterFragmentQName = null;
	}

	/**
	 * Disabling partial martial fragment write mode.
	 */
	public void stopWritingPartialMasterFragment() {
		writingPartialMasterFragment = false;
		masterFragmentQName = null;
	}

	@Override
	public void add(XMLEvent event) throws XMLStreamException {
		if (writingPartialMasterFragment && event.isStartElement() && masterFragmentQName == null) {
			masterFragmentQName = event.asStartElement().getName();
		} else if (writingPartialMasterFragment && event.isEndElement()) {
			if (event.asEndElement().getName().equals(masterFragmentQName)) {
				// filter end element of master fragment
				return;
			}
		}
		super.add(event);
	}

}