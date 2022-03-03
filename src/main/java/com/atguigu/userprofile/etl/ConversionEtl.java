package com.atguigu.userprofile.etl;

import com.atguigu.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class ConversionEtl {

    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();
        ConversionVo conversionVo = conversionCount(session);

        System.out.println(conversionVo);

    }

    static ConversionVo conversionCount(SparkSession session){

        // 查询下过订单的用户
        Dataset<Row> orderSql = session.sql("select distinct(member_id)  from ecommerce.t_order where order_status=2");

        // 将购买次数超过 1 次的用户查出来
        Dataset<Row> orderAgianSql = session.sql("SELECT t.member_id member_id FROM(SELECT COUNT(order_id) orderCount,member_id FROM  ecommerce.t_order WHERE order_status=2 GROUP BY member_id) t WHERE t.orderCount>1");

        // 查询充值过的用户
        Dataset<Row> couponSql = session.sql("select distinct(member_id) member_id from ecommerce.t_coupon_member where coupon_channel=1");

        Dataset<Tuple2<Row, Row>> join = orderAgianSql.joinWith(couponSql,
                orderAgianSql.col("member_id").equalTo(couponSql.col("member_id")),
                "inner");

        long orderCount = orderSql.count();
        long orderAgainCount = orderAgianSql.count();
        long chargeCoupon = join.count();

        ConversionVo conversionVo = new ConversionVo();
        conversionVo.setPresent(1000L);
        conversionVo.setClick(800L);
        conversionVo.setAddCart(600L);
        conversionVo.setOrder(orderCount);
        conversionVo.setOrderAgain(orderAgainCount);
        conversionVo.setChargeCoupon(chargeCoupon);

        return conversionVo;
    }

    @Data
    static  class  ConversionVo{
        private Long present;
        private Long click;
        private Long addCart;
        private Long order;
        private Long orderAgain;
        private Long chargeCoupon;
    }

}
