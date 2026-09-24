package com.portfolio.notifyhub;

import org.springframework.boot.SpringApplication;

public class TestNotifyhubApplication {

	public static void main(String[] args) {
		SpringApplication.from(NotifyhubApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
