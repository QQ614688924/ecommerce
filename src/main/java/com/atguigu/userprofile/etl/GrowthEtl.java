package com.atguigu.userprofile.etl;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.userprofile.utils.DateStyle;
import com.atguigu.userprofile.utils.DateUtil;
import com.atguigu.userprofile.utils.SparkUtils;
import com.google.inject.internal.cglib.core.$DuplicatesPredicate;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GrowthEtl {

    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();

        List<GrowthLineVo> growthLineVos = growthLineEtl(session);


    }

    private static List<GrowthLineVo> growthLineEtl(SparkSession session){
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);

        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        Date sevenDayBefore = DateUtil.addDay(nowDay, -7);


        String memberSql = "select date_format(create_time,'yyyy-MM-dd') day,count(id) regCount from ecommerce.t_member" +
                " where create_time >= '%s' group by date_format(create_time,'yyyy-MM-dd') order by day ";
        memberSql = String.format(memberSql,DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));



        Dataset<Row> ds1 = session.sql(memberSql);


        String gmvSql = "select  date_format(create_time,'yyyy-MM-dd') day,count(order_id) orderCount,sum(origin_price) gmv" +
                " from ecommerce.t_order where create_time >= '%s' group by date_format(create_time,'yyyy-MM-dd') order by day ";
        gmvSql = String.format(gmvSql,DateUtil.DateToString(sevenDayBefore,DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> ds2 = session.sql(gmvSql);

        Dataset<Tuple2<Row, Row>> dataset = ds1.joinWith(ds2, ds1.col("day").equalTo(ds2.col("day")), "inner");

        List<Tuple2<Row, Row>> tuple2s = dataset.collectAsList();
        System.out.println(tuple2s);


        ArrayList<GrowthLineVo> vos = new ArrayList<>();

        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            Row row1 = tuple2._1;
            Row row2 = tuple2._2;

            JSONObject obj = new JSONObject();

            StructType schema1 = row1.schema();

            String[] strings = schema1.fieldNames();

            for (String string : strings) {
                Object o = row1.getAs(string);
                obj.put(string,o);
            }

            StructType schema2 = row2.schema();
            String[] strings1 = schema2.fieldNames();
            for (String s : strings1) {
                Object as = row2.getAs(s);
                obj.put(s,as);
            }

            vos.add(obj.toJavaObject(GrowthLineVo.class));

        }
        String totalCountSql = "select count(id) preGmv from ecommerce.t_member where create_time < '%s' ";

        totalCountSql = String.format(totalCountSql,DateUtil.DateToString(sevenDayBefore,DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> ds4 = session.sql(totalCountSql);

        Long aLong = ds4.collectAsList().get(0).getLong(0);


        String preGmvSql = "select sum(origin_price) preGmv from ecommerce.t_order where create_time < '%s' ";

        preGmvSql = String.format(preGmvSql,DateUtil.DateToString(sevenDayBefore,DateStyle.YYYY_MM_DD_HH_MM_SS));

        Dataset<Row> ds3 = session.sql(preGmvSql);

        BigDecimal preGmv = BigDecimal.valueOf(ds3.collectAsList().get(0).getDouble(0));

        BigDecimal currentGmv = preGmv;

        Long currentReg = aLong;

        for (int i = 0; i < vos.size(); i++) {
            GrowthLineVo vo = vos.get(i);
            BigDecimal gmv = vo.getGmv();
            int regCount = vo.getRegCount();
            currentReg = currentReg + regCount;

            currentGmv = currentGmv.add(gmv);

            vo.setGmv(currentGmv);


        }

        return vos;
    }


    @Data
    static class GrowthLineVo{

        private String day;
        private Integer regCount;
        private Integer memberCount;
        private Integer orderCount;
        private BigDecimal gmv;

    }

}