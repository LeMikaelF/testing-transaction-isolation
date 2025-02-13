package com.mikaelfrancoeur.mysqltransactiontest;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.assertj.core.api.WithAssertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import lombok.SneakyThrows;

@SpringBootTest
class MysqlTransactionTestApplicationTests implements WithAssertions {

    private static final int NUM_EXPECTED_DEADLOCKS = 1;
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
        for (int numDeadlocks = 0; numDeadlocks < NUM_EXPECTED_DEADLOCKS; ) {
            Exception deadlock = catchThrowableOfType(CannotAcquireLockException.class, this::raceSqlThreads);

            if (deadlock != null) {
                numDeadlocks++;
                continue;
            }

            assertNoPhantomRead();
        }
    }

    private void raceSqlThreads() throws InterruptedException, BrokenBarrierException {
        jdbcTemplate.execute("truncate t");

        // language=SQL
        String insertSql = "insert into t select 1 where not exists (select * from t where num = 1)";

        CyclicBarrier barrier = new CyclicBarrier(2);

        Thread otherThread = new Thread(() ->
                transactionTemplate.execute(throwing(_ -> {
                    barrier.await();
                    Thread.sleep(1);
                    jdbcTemplate.execute(insertSql);
                    return null;
                })));

        otherThread.start();
        barrier.await();
        jdbcTemplate.execute(insertSql);

        otherThread.join();
    }

    private void assertNoPhantomRead() {
        Integer count = jdbcTemplate.queryForObject("select count(*) from t where num = 1", Integer.class);
        assertThat(count)
                .withFailMessage("found phantom read, transaction is not properly isolated")
                .isEqualTo(1);
    }

    private interface ThrowingCallback<T> extends TransactionCallback<T> {
        @Override
        @SneakyThrows
        default T doInTransaction(@NotNull TransactionStatus status) {
            return doThrowing(status);
        }

        T doThrowing(TransactionStatus status) throws Exception;
    }

    private <T> TransactionCallback<T> throwing(ThrowingCallback<T> callback) {
        return callback;
    }
}
