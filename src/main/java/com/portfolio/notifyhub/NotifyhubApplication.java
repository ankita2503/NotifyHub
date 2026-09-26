package com.portfolio.notifyhub;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NotifyhubApplication {

	public static void main(String[] args) {
		SpringApplication.run(NotifyhubApplication.class, args);
	}

}
