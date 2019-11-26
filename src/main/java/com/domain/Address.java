package com.domain;

import com.google.auto.value.AutoValue.Builder;

import lombok.Data;

@Data
@Builder
public class Address {

	String line1;
    String line2;
    String state;
    String district;
    String country;
    long pinCode;
public Address() {
	// TODO Auto-generated constructor stub
}
    public Address(String line1, String line2, String state, String district, String country, long pinCode) {
        this.line1 = line1;
        this.line2 = line2;
        this.state = state;
        this.district = district;
        this.country = country;
        this.pinCode = pinCode;
    }

    public String getLine1() {
        return line1;
    }

    public void setLine1(String line1) {
        this.line1 = line1;
    }

    public String getLine2() {
        return line2;
    }

    public void setLine2(String line2) {
        this.line2 = line2;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDistrict() {
        return district;
    }

    public void setDistrict(String district) {
        this.district = district;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public long getPinCode() {
        return pinCode;
    }

    public void setPinCode(long pinCode) {
        this.pinCode = pinCode;
    }
	
	
}
