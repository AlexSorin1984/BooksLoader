package com.alexsorin.booksloader.author;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class AuthorService {
    @Autowired
    @Lazy
    AuthorRepository authorRepository;

    public void saveAuthor(Author author){
        authorRepository.save(author);
    }

    public Optional<Author> findById(String id){return authorRepository.findById(id);}

}
