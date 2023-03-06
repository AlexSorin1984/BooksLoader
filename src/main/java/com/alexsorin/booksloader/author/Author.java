package com.alexsorin.booksloader.author;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table(value = "author_by_id")
public class Author {
    @Id
    @PrimaryKeyColumn(name="author_id", ordinal = 0, type= PrimaryKeyType.PARTITIONED)
    private String author_id;
    @Column("author_name")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String author_name;
    @Column("personal_name")
    @CassandraType(type = CassandraType.Name.TEXT)
    private String personal_name;

    public String getAuthor_id() {
        return author_id;
    }

    public void setAuthor_id(String author_id) {
        this.author_id = author_id;
    }

    public String getAuthor_name() {
        return author_name;
    }

    public void setAuthor_name(String author_name) {
        this.author_name = author_name;
    }

    public String getPersonal_name() {
        return personal_name;
    }

    public void setPersonal_name(String personal_name) {
        this.personal_name = personal_name;
    }
}
