package com.atguigu.userprofile.etl;

import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.utils.DateStyle;
import com.atguigu.userprofile.utils.DateUtil;
import com.atguigu.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class WowEtl {

    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();

        List<RegVo> regVos = regWeekCount(session);
        List<OrderVo> orderVos = orderWeekCount(session);

        System.out.println(regVos);
        System.out.println(orderVos);

    }

    static List<RegVo> regWeekCount(SparkSession session) {

        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);

        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        Date lastTwoWeekFirstDay = DateUtil.addDay(nowDay, -14);

        String regSql = "select date_format(create_time,'yyyy-MM-dd') day," +
                "count(id) regCount from ecommerce.t_member where create_time>='%s' and create_time < '%s'" +
                "group by date_format(create_time,'yyyy-MM-dd')";

        regSql = String.format(regSql, DateUtil.DateToString(lastTwoWeekFirstDay, DateStyle.YYYY_MM_DD_HH_MM_SS), DateUtil.DateToString(nowDay, DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> dataset = session.sql(regSql);

        List<String> collect = dataset.toJSON().collectAsList();

        List<RegVo> regVos = collect.stream().map(str -> JSON.parseObject(str, RegVo.class)).collect(Collectors.toList());

        return regVos;
    }

    static List<OrderVo> orderWeekCount(SparkSession session) {

        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);

        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        Date lastTwoWeekFirstDay = DateUtil.addDay(nowDay, -14);

        String orderSql = "select date_format(create_time,'yyyy-MM-dd') day," +
                "count(order_id) orderCount from ecommerce.t_order where create_time>='%s' and create_time < '%s'" +
                "group by date_format(create_time,'yyyy-MM-dd')";

        orderSql = String.format(orderSql, DateUtil.DateToString(lastTwoWeekFirstDay, DateStyle.YYYY_MM_DD_HH_MM_SS), DateUtil.DateToString(nowDay, DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> dataset = session.sql(orderSql);

        List<String> collect = dataset.toJSON().collectAsList();

        List<OrderVo> orderVos = collect.stream().map(str -> JSON.parseObject(str, OrderVo.class)).collect(Collectors.toList());

        return orderVos;
    }

    @Data
    static class RegVo {
        private String day;
        private Integer regCount;
    }

    @Data
    static class OrderVo {
        private String day;
        private Integer orderCount;
    }

}



