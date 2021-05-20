package com.example.esdemo.excellistener;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.util.CollectionUtils;
import com.example.esdemo.entity.Student;
import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Describe：创建读取student监听器
 * @Author： shuaihu4
 * @Data: 2021年05月20日 16:30
 */
public class StudentListener extends AnalysisEventListener<Student> {

    /**
     * 批量读取条数
     */
    private static final int BATCH_COUNT = 5;
    private static AtomicInteger total = new AtomicInteger(0);
    /**
     * 创建ES客户端
     */
    private RestHighLevelClient client;
    private ArrayList<Student> list = new ArrayList<Student>();

    public StudentListener(RestHighLevelClient client) {
        this.client=client;
    }

    /**
     * 每解析一条数据都会调用这个方法
     * @param student
     * @param analysisContext
     */
    @Override
    public void invoke(Student student, AnalysisContext analysisContext) {
        //计数自增
        total.incrementAndGet();
        //将数据添加到list
        list.add(student);
        //判断计数达到指定值
        if(BATCH_COUNT <= total.get()){
            //ES保存数据 todo
            saveToEs(list);
            //清除list
            list.clear();
        }
    }


    /**
     * 所有数据解析完成会回调这个方法
     * @param analysisContext
     */
    @Override
    public void doAfterAllAnalysed(AnalysisContext analysisContext) {
        // 保存到数据
        saveToEs(list);
        System.out.println("数据导入完毕！");
    }

    private void saveToEs(List<Student> list){
        if (CollectionUtils.isEmpty(list)) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        for (Student student : list) {
            try {
                bulkRequest.add(new IndexRequest("student").source(BeanUtils.describe(student)));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
        BulkResponse bulk = null;
        try {
            bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if(bulk.status().getStatus()==200){
                System.out.println("批量插入成功!");
            }
        } catch (IOException e) {
            System.out.println("插入失败！");
            e.printStackTrace();
        }

    }
}
