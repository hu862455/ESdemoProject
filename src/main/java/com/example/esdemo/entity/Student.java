package com.example.esdemo.entity;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;

import java.math.BigDecimal;

/**
 * @Describe：
 * @Author： shuaihu4
 * @Data: 2021年05月20日 16:25
 */
@Data
public class Student {
    @ExcelProperty("姓名") //ExcelProperty代表导出列表头
    private String name;
    @ExcelProperty("年龄")
    private Integer age;
    @ExcelProperty("成绩")
    private BigDecimal score;
}
