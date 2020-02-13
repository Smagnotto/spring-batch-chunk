package br.com.fiap.springbatchchunk;

import br.com.fiap.springbatchchunk.pojo.PessoaPOJO;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;


@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchChunkApplication {

	@Bean
	public FlatFileItemReader<PessoaPOJO> fileReader(@Value("${input.file}") Resource resource) {
		return new FlatFileItemReaderBuilder<PessoaPOJO>()
				.name("read file")
				.resource(resource)
				.targetType(PessoaPOJO.class)
				.delimited().delimiter(";").names("nome", "cpf")
				.build();
	}

	@Bean
	public ItemProcessor<PessoaPOJO, PessoaPOJO> processor() {
		return pessoa -> {
			pessoa.setNome(pessoa.getNome().toUpperCase());
			pessoa.setCpf(pessoa.getCpf().replaceAll("\\.", "").replace("-", ""));
			return pessoa;
		};
	}

	@Bean
	public JdbcBatchItemWriter databaseWriter(DataSource datasource) {
		return new JdbcBatchItemWriterBuilder<PessoaPOJO>()
				.dataSource(datasource)
				.sql("insert into TB_PESSOA (NOME, CPF) values (:nome, :cpf)")
				.beanMapped()
				.build();
	}

	@Bean
	public Step step(StepBuilderFactory stepBuilderFactory,
					 ItemReader<PessoaPOJO> itemReader,
					 ItemWriter<PessoaPOJO> itemWriter,
					 ItemProcessor<PessoaPOJO, PessoaPOJO> processor) {

		return stepBuilderFactory.get("arquivo-bd")
				.<PessoaPOJO, PessoaPOJO>chunk(100)
				.reader(itemReader)
				.processor(processor)
				.writer(itemWriter)
				.build();
	}

	@Bean
	public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
		return jobBuilderFactory.get("nomeDoJob")
				.start(step)
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchChunkApplication.class, args);
	}

}
