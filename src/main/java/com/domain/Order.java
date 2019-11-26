package com.domain;

import com.google.auto.value.AutoValue.Builder;

import lombok.Data;

@Data
@Builder
public class Order {

	
	int id;
    Customer customer;
    Double orderTotal;
    String status;
    Address shippingAddress;
    Address billingAddress;
    boolean isGift;
    
public Order() {

}
    public Order(int id, Customer customer, Double orderTotal, String status, Address shippingAddress, Address billingAddress, boolean isGift) {
        this.id = id;
        this.customer = customer;
        this.orderTotal = orderTotal;
        this.status = status;
        this.shippingAddress = shippingAddress;
        this.billingAddress = billingAddress;
        this.isGift = isGift;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public Double getOrderTotal() {
        return orderTotal;
    }

    public void setOrderTotal(Double orderTotal) {
        this.orderTotal = orderTotal;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Address getShippingAddress() {
        return shippingAddress;
    }

    public void setShippingAddress(Address shippingAddress) {
        this.shippingAddress = shippingAddress;
    }

    public Address getBillingAddress() {
        return billingAddress;
    }

    public void setBillingAddress(Address billingAddress) {
        this.billingAddress = billingAddress;
    }

    public boolean isGift() {
        return isGift;
    }

    public void setGift(boolean gift) {
        isGift = gift;
    }
	
	
}
