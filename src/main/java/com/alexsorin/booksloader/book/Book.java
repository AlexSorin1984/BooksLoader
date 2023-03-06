package com.alexsorin.booksloader.book;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.*;

import java.time.LocalDate;
import java.util.List;

@Table(value="book_by_id")
public class Book {

    @Id @PrimaryKeyColumn(name="book_id", ordinal=0, type= PrimaryKeyType.PARTITIONED)
    private String book_id;

    @Column("book_name")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String book_name;

    @Column("book_description")
    @CassandraType(type=CassandraType.Name.TEXT)
    private String book_description;

    @Column("published_date")
    @CassandraType(type=CassandraType.Name.DATE)
    private LocalDate published_date;

    @Column("cover_ids")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.TEXT)
    private List<String> cover_ids;

    @Column("author_ids")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.TEXT)
    private List<String> author_ids;

    @Column("author_names")
    @CassandraType(type = CassandraType.Name.LIST, typeArguments = CassandraType.Name.TEXT)
    private List<String> author_names;



    public String getBook_id() {
        return book_id;
    }

    public void setBook_id(String book_id) {
        this.book_id = book_id;
    }

    public String getBook_name() {
        return book_name;
    }

    public void setBook_name(String book_name) {
        this.book_name = book_name;
    }

    public String getBook_description() {
        return book_description;
    }

    public void setBook_description(String book_description) {
        this.book_description = book_description;
    }

    public LocalDate getPublished_date() {
        return published_date;
    }

    public void setPublished_date(LocalDate published_date) {
        this.published_date = published_date;
    }

    public List<String> getCover_ids() {
        return cover_ids;
    }

    public void setCover_ids(List<String> cover_ids) {
        this.cover_ids = cover_ids;
    }

    public List<String> getAuthor_names() {
        return author_names;
    }

    public void setAuthor_names(List<String> author_names) {
        this.author_names = author_names;
    }

    public List<String> getAuthor_ids() {
        return author_ids;
    }

    public void setAuthor_ids(List<String> author_ids) {
        this.author_ids = author_ids;
    }
}
