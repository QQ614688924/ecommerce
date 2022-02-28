package com.atguigu.userprofile.etl;
import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.utils.*;
import lombok.Data;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.*;

import java.util.List;
import java.util.stream.Collectors;

public class TestEtl {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);

        SparkSession session = SparkUtils.initSession();

        Dataset<Row> dataSet = session.sql("select sex as memberSex,count(id) as sexCount from ecommerce.t_member group by sex");

        List<String> list = dataSet.toJSON().collectAsList();

//        for (String s : list) {
//            System.out.println(s);
//        }
        List<MemberSex> result = list.stream().map(str -> JSON.parseObject(str, MemberSex.class)).collect(Collectors.toList());

        System.out.println(result);


    }

    @Data
    static class MemberSex{
        private Integer memberSex;
        private Integer sexCount;
    }

}
