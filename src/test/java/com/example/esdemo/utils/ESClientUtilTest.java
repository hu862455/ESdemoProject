package com.example.esdemo.utils;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;


@SpringBootTest
class ESClientUtilTest {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;


    @Test
    private void createIndex() throws IOException {
        CreateIndexRequest hushuai = new CreateIndexRequest("hushuai");
        CreateIndexResponse createIndexResponse = client.indices().create(hushuai, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

}