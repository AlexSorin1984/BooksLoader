package com.alexsorin.booksloader.book;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

@Service
public class BookService {

    @Autowired
    @Lazy
    BookRepository bookRepository;

    public void save(Book book){bookRepository.save(book);}
}
