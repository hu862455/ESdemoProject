package com.example.esdemo.utils;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.io.IOException;

/**
 * @Describe：
 * @Author： shuaihu4
 * @Data: 2021年05月12日 17:36
 */
public class ESClientUtil {

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;


    private void createIndex() throws IOException {
        CreateIndexRequest hushuai = new CreateIndexRequest("hushuai");
        CreateIndexResponse createIndexResponse = client.indices().create(hushuai, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

}
