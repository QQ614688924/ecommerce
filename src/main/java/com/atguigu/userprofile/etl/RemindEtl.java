package com.atguigu.userprofile.etl;

import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.utils.DateStyle;
import com.atguigu.userprofile.utils.DateUtil;
import com.atguigu.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class RemindEtl {

    public static void main(String[] args) {
//        Logger.getLogger("org").setLevel(Level.OFF);
//        Logger.getLogger("com").setLevel(Level.OFF);
//        System.setProperty("spark.ui.showConsoleProgress", "false");
//        Logger.getRootLogger().setLevel(Level.OFF);

        SparkSession session = SparkUtils.initSession();

        List<FreeRemindVo> freeRemindVos = freeRemindCount(session);
        List<CouponRemindVo> couponRemindVos = couponRemindCount(session);

        System.out.println(freeRemindVos);
        System.out.println(couponRemindVos);

    }

    static List<FreeRemindVo> freeRemindCount(SparkSession session){

        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);

        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        Date sevenDay = DateUtil.addDay(nowDay, -7);

        String freeRemindSql ="select date_format(create_time,'yyyy-MM-dd') day," +
                "count(member_id) as freeCount from ecommerce.t_coupon_member where create_time>='%s' and coupon_id = 1 and coupon_channel =2 " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        freeRemindSql = String.format(freeRemindSql, DateUtil.DateToString(sevenDay, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(freeRemindSql);

        List<FreeRemindVo> result = dataset.toJSON().collectAsList().stream().map(str -> JSON.parseObject(str, FreeRemindVo.class)).collect(Collectors.toList());

        return result;
    }

    static List<CouponRemindVo> couponRemindCount(SparkSession session){

        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);

        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        Date sevenDay = DateUtil.addDay(nowDay, -7);

        String couponRemindSql ="select date_format(create_time,'yyyy-MM-dd') day," +
                "count(member_id) as couponCount from ecommerce.t_coupon_member where create_time>='%s' and coupon_id != 1 " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        couponRemindSql = String.format(couponRemindSql, DateUtil.DateToString(sevenDay, DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> dataset = session.sql(couponRemindSql);

        List<CouponRemindVo> result = dataset.toJSON().collectAsList().stream().map(str -> JSON.parseObject(str, CouponRemindVo.class)).collect(Collectors.toList());

        return result;
    }


    @Data
    static class FreeRemindVo{
        private String day;
        private Integer freeCount;
    }

    @Data
    static class CouponRemindVo{
        private String day;
        private Integer couponCount;
    }

}
