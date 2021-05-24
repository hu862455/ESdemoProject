package com.example.esdemo.utils;

import org.apache.log4j.Logger;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * @Describe：
 * @Author： shuaihu4
 * @Data: 2021年05月21日 10:46
 */
public class EsUtils {

    private static Logger LOGGER = Logger.getLogger(EsUtils.class);

    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    private static RestHighLevelClient client;

    /**
     * 容器初始化时执行
     */
    @PostConstruct
    public void init() {
        // 将容器中的restHighLevelClient放入类中
        client = this.restHighLevelClient;
    }
    /**
     * 获取低级客户端
     * @author shuaihu4
     * @date 2021/5/21 11:04
     * @return org.elasticsearch.client.RestClient
     */
    public static RestClient getLowLevelClient(){
        return client.getLowLevelClient();
    }

    /**
     * 创建文档
     * @param index 索引名
     * @param josonStr 文档json字符串
     * @return
     * @throws Exception
     */
    public static boolean createDocument(String index, String josonStr) throws Exception {
        return createDocument(index, null, josonStr);
    }

    /**
     * 创建文档
     * @param index 索引名
     * @param map 文档map
     * @return
     * @throws Exception
     */
    public static boolean createDocument(String index, Map<String, Object> map) throws Exception {
        return createDocument(index, null, map);
    }

    /**
     * 创建指定ID文档：jsonStr
     * 若documentId已存在 则更新文档
     * @author shuaihu4
     * @date 2021/5/21 11:30
     * @param index 索引名
     * @param documentId documentId推荐为空，由ES自动创建
     * @param josonStr 文档json字符串
     * @return boolean
     */
    public static boolean createDocument(String index, String documentId, String josonStr) throws Exception {
        IndexRequest request = new IndexRequest(index).id(documentId).source(josonStr, XContentType.JSON);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED
                || indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            return true;
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            return true;
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo
                    .getFailures()) {
                throw new Exception(failure.reason());
            }
        }
        return false;
    }


    /**
     * 创建指定ID文档：jsonStr
     * documentId已存在 则更新文档
     * @author shuaihu4
     * @date 2021/5/21 11:31
     * @param index 索引名
     * @param documentId documentId推荐为空，由ES自动创建
     * @param map 文档Map
     * @return boolean
     */
    public static boolean createDocument(String index, String documentId, Map<String,Object> map) throws Exception {
        IndexRequest request = new IndexRequest(index).id(documentId).source(map);
        IndexResponse indexResponse = client.index(request,RequestOptions.DEFAULT);
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED
                || indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            return true;
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            return true;
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo
                    .getFailures()) {
                throw new Exception(failure.reason());
            }
        }
        return false;
    }

    /**
     * 删除文档 默认doc文档
     *
     * @param index 索引名
     * @param documentId 文档id
     * @return 删除成功：true 删除失败：false
     * @throws Exception
     */
    public static boolean deleteDocument(String index, String documentId) throws Exception {
        DeleteRequest request = new DeleteRequest(index, documentId);
        DeleteResponse deleteResponse = client.delete(request,RequestOptions.DEFAULT);
        if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            return false;
        }
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            return false;
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo
                    .getFailures()) {
                throw new Exception(failure.reason());
            }
        }
        return true;
    }

    /**
     * 验证文档是否存在
     * @author shuaihu4
     * @date 2021/5/21 11:55
     * @param index 索引名
     * @param documentId 文档id
     * @return boolean
     */
    public static boolean documentExists(String index, String documentId) throws Exception {
        GetRequest getRequest = new GetRequest(index,documentId);
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }
}
