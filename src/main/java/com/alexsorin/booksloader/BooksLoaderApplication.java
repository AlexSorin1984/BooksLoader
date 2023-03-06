package com.alexsorin.booksloader;


import com.alexsorin.booksloader.author.Author;
import com.alexsorin.booksloader.author.AuthorService;
import com.alexsorin.booksloader.book.Book;
import com.alexsorin.booksloader.book.BookService;
import connection.DataStaxAstraProperties;
import connection.SpringDataCassandraProperties;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@SpringBootApplication
@EnableConfigurationProperties({DataStaxAstraProperties.class, SpringDataCassandraProperties.class})
public class BooksLoaderApplication {

	@Autowired
	public AuthorService authorService;

	@Autowired
	public BookService bookService;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;



	public static void main(String[] args) {
		 SpringApplication.run(BooksLoaderApplication.class, args);

	}

	private void initAuthors() throws IOException {
		Path path = Paths.get(authorDumpLocation);
		Stream<String> lines = Files.lines(path);
		lines.forEach(line->{
			String jsonString = line.substring(line.indexOf("{"));
			JSONObject jsonObject = new JSONObject(jsonString);
			Author author = new Author();
			author.setAuthor_id(jsonObject.optString("key").replace("/authors/",""));
			author.setAuthor_name(jsonObject.optString("name"));
			author.setPersonal_name(jsonObject.optString("personal_name"));
			authorService.saveAuthor(author);
		});


	}

	private void initWorks() throws IOException {
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		Stream<String> lines = Files.lines(path);
		lines.forEach(line->{
			String jsonString = line.substring(line.indexOf("{"));
			JSONObject jsonObject = new JSONObject(jsonString);
			Book book = new Book();
			book.setBook_id(jsonObject.getString("key").replace("/works/", ""));
			book.setBook_name(jsonObject.optString("title"));
			JSONObject descriptionObj = jsonObject.optJSONObject("description");
			if (descriptionObj != null){
				book.setBook_description(descriptionObj.optString("value"));
			}
			JSONObject createdObject = jsonObject.optJSONObject("created");
			if(createdObject!=null){
				String dateString = createdObject.optString("value");
				book.setPublished_date(LocalDate.parse(dateString, dateFormat	));
			}
			JSONArray coversJsonArr = jsonObject.optJSONArray("covers");
			if(coversJsonArr!=null){
				List<String> coverIds = new ArrayList<String>();
				for(int i=0; i<coversJsonArr.length(); i++){
					coverIds.add(coversJsonArr.optString(i));
				}
				book.setCover_ids(coverIds);
			}

			JSONArray authorsJsonArr = jsonObject.optJSONArray("authors");
			if(authorsJsonArr!=null){
				List<String> authorIds = new ArrayList<String>();
				for(int i=0; i<authorsJsonArr.length(); i++){
					String authorId = authorsJsonArr.getJSONObject(i).getJSONObject("author").getString("key")
							.replace("/authors/", "");
					authorIds.add(authorId);
				}
				book.setAuthor_ids(authorIds);
				List<String> authorNames = authorIds.stream()
						.map(authorId ->authorService.findById(authorId))
						.map(optionalAuthor->{
							if(!optionalAuthor.isPresent()) return "Unknown author";
							return optionalAuthor.get().getAuthor_name();
						}).collect(Collectors.toList());
				book.setAuthor_names(authorNames);
			}
			bookService.save(book);
		});
	}

	@EventListener(ApplicationReadyEvent.class)
	public void start() throws IOException {
		initAuthors();
		initWorks();
		System.out.println(authorDumpLocation);
		Author author = new Author();
		author.setAuthor_id("id2");
		author.setAuthor_name("name2");
		author.setPersonal_name("personalName3");
		authorService.saveAuthor(author);
	}



	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizers(DataStaxAstraProperties astraProperties, SpringDataCassandraProperties cassandraProperties) {
		return builder -> builder.withCloudSecureConnectBundle(astraProperties.getSecureConnectBundle().toPath())
				                  .withKeyspace("main")
							     .withAuthCredentials(cassandraProperties.getUsername(), cassandraProperties.getPassword());


	}
}
