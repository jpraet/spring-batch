package org.springframework.batch.item;

import org.springframework.batch.item.ItemCountAware;


/**
 * Generic MasterDetailItem wrapper class.
 * 
 * @param <M>
 *            the master item type
 * @param <D>
 *            the detail item type
 */
public class MasterDetailItem<M, D> implements ItemCountAware {

	private M master;

	private D detail;

	private int itemCount;

	private int masterCount;

	private int detailCount;

	public M getMaster() {
		return master;
	}

	public void setMaster(M master) {
		this.master = master;
	}

	public D getDetail() {
		return detail;
	}

	public void setDetail(D detail) {
		this.detail = detail;
	}

	public int getItemCount() {
		return itemCount;
	}

	@Override
	public void setItemCount(int itemCount) {
		this.itemCount = itemCount;
	}

	public int getMasterCount() {
		return masterCount;
	}

	public void setMasterCount(int masterCount) {
		this.masterCount = masterCount;
	}

	public int getDetailCount() {
		return detailCount;
	}

	public void setDetailCount(int detailCount) {
		this.detailCount = detailCount;
	}

	/**
	 * Returns whether or not this MasterDetailItem is a new master item.
	 * This is true when this MasterDetailItem has no detail items, or when it contains the first detail item.
	 * 
	 * @return whether or not this MasterDetailItem is a new master item
	 */
	public boolean isNewMasterItem() {
		return detail == null || detailCount == 1;
	}

	@Override
	public String toString() {
		return "MasterDetailItem [master=" + master + ", detail=" + detail + ", itemCount=" + itemCount
				+ ", masterCount=" + masterCount + ", detailCount=" + detailCount + "]";
	}
	
}
