package com.domain;

import com.google.auto.value.AutoValue.Builder;

import lombok.Data;

@Data
@Builder
public class Customer {

	
	int id;
    String name;
    int age;
    String gender;
public Customer() {
	// TODO Auto-generated constructor stub
}
    public Customer(int id, String name, int age, String gender, boolean isPrime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.gender = gender;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }
	
	
	
}

