package br.com.fiap.springbatchchunk;

import br.com.fiap.springbatchchunk.config.BatchConfig;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;


@SpringBootTest
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpringBatchChunkApplication.class,
		BatchConfig.class})

class SpringBatchChunkApplicationTests {
	@Autowired
	private JobLauncherTestUtils jobLaucherTest;

	@Autowired
	private Job job;

	@Autowired
	private DataSource dataSource;

	@Test
	public void testJob() throws Exception {
		JobExecution jobExecution = jobLaucherTest.getJobLauncher()
				.run(job, jobLaucherTest.getUniqueJobParameters());

		Assertions.assertNotNull(jobExecution);
		Assertions.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

		ResultSet resultSet = dataSource.getConnection()
				.prepareStatement("SELECT count(*) from TB_PESSOA")
				.executeQuery();

		Awaitility.await().atMost(10, TimeUnit.SECONDS)
				.until(() -> {
					resultSet.last();
					return resultSet.getInt(1) == 3;
				});

		Assertions.assertEquals(3, resultSet.getInt(1));
	}
}
