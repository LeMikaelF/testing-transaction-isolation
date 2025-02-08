package com.mikaelfrancoeur.mysqltransactiontest;

import org.assertj.core.api.WithAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import lombok.SneakyThrows;

@SpringBootTest
class MysqlTransactionTestApplicationTests implements WithAssertions {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void beforeEach() {
        transactionTemplate = new TransactionTemplate(platformTransactionManager);
        // uncomment this to make the test fail
        // transactionTemplate.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);

        jdbcTemplate.execute("drop table if exists t");
        jdbcTemplate.execute("create table t (num int)");
    }

    @Test
    @SneakyThrows
    void testIsolation() {
        for (int numDeadlocks = 0; numDeadlocks < 1; ) {
            Exception deadlock = catchThrowableOfType(CannotAcquireLockException.class, this::raceSqlThreads);

            if (deadlock != null) {
                numDeadlocks++;
                continue;
            }

            assertNoPhantomRead();
        }
    }

    private void raceSqlThreads() throws InterruptedException {
        jdbcTemplate.execute("truncate t");

        // language=SQL
        String insertSql = "insert into t select (select 1 where not exists (select * from t where num = 1))";

        Thread otherThread = new Thread(() ->
                transactionTemplate.execute(_ -> {
                    jdbcTemplate.execute(insertSql);
                    return null;
                }));

        otherThread.start();
        jdbcTemplate.execute(insertSql);

        otherThread.join();
    }

    private void assertNoPhantomRead() {
        Integer count = jdbcTemplate.queryForObject("select count(*) from t where num = 1", Integer.class);
        assertThat(count)
                .withFailMessage("found phantom read, transaction is not properly isolated")
                .isEqualTo(1);
    }

}
