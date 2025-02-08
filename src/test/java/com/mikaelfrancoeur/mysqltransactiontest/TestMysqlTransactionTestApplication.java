package com.mikaelfrancoeur.mysqltransactiontest;

import org.springframework.boot.SpringApplication;

public class TestMysqlTransactionTestApplication {

    public static void main(String[] args) {
        SpringApplication.from(MysqlTransactionTestApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
