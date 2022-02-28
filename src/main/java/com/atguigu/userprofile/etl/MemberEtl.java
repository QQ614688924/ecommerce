package com.atguigu.userprofile.etl;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import com.atguigu.userprofile.utils.*;
import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {


    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();

        List<MemberSex> memberSexs = memberSexEtl(session);
        List<MemberChannel> memberChannels = memberChannelEtl(session);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(session);
        MemberHeat memberHeat = MemberHeatEtl(session);


        MemberVo memberVo = new MemberVo();

        memberVo.setMemberSexs(memberSexs);
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberHeats(memberHeat);

        System.out.println("===========打印控制台==========="+JSON.toJSONString(memberVo));


    }

    static List<MemberSex> memberSexEtl(SparkSession session){

        Dataset<Row> dataset = session.sql("select sex as memberSex,count(id) as sexCount from ecommerce.t_member group by sex");

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberSex> memberSexList = list.stream().map(str -> JSON.parseObject(str, MemberSex.class)).collect(Collectors.toList());

        return memberSexList;

    }

    static List<MemberChannel> memberChannelEtl(SparkSession session){

        Dataset<Row> dataset = session.sql("SELECT member_channel AS memberChannel,COUNT(id) AS channelCount FROM  ecommerce.t_member GROUP BY member_channel");

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberChannel> memberChannelList = list.stream().map(str -> JSON.parseObject(str, MemberChannel.class)).collect(Collectors.toList());

        return memberChannelList;
    }

    static List<MemberMpSub> memberMpSubEtl(SparkSession session){

        Dataset<Row> dataset = session.sql("SELECT  COUNT(IF (mp_open_id !='null',1,NULL)) AS subCount,\n" +
                "\tCOUNT(IF (mp_open_id ='null',1,NULL)) AS unSubCount\n" +
                "FROM  ecommerce.t_member");

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberMpSub> memberMpSubList = list.stream().map(str -> JSON.parseObject(str, MemberMpSub.class)).collect(Collectors.toList());

        return memberMpSubList;
    }

    static MemberHeat MemberHeatEtl(SparkSession session){

        Dataset<Row> reg_complete = session.sql("SELECT COUNT(IF(phone IS NULL,1,NULL)) AS reg ,COUNT(IF(phone IS NOT NULL,1,NULL)) AS complete  FROM ecommerce.t_member");

        Dataset<Row> order_again = session.sql("SELECT \n" +
                "\tCOUNT(IF(order_count=1,1,NULL)) AS `order`,\n" +
                "\tCOUNT(IF(order_count>=2,1,NULL)) AS orderAgain\n" +
                "FROM (SELECT COUNT(order_id) AS order_count,member_id FROM ecommerce.t_order GROUP BY member_id) AS t ");

        Dataset<Row> coupon = session.sql("SELECT COUNT(DISTINCT member_id) AS coupon FROM ecommerce.t_coupon_member");

        Dataset<Row> dataset = reg_complete.crossJoin(order_again).crossJoin(coupon);


        List<String> list = dataset.toJSON().collectAsList();

        List<MemberHeat> MemberHeatList = list.stream().map(str -> JSON.parseObject(str, MemberHeat.class)).collect(Collectors.toList());

        return MemberHeatList.get(0);
    }



    @Data
    static class  MemberVo{
        private List<MemberSex> memberSexs;
        private List<MemberChannel> memberChannels;
        private List<MemberMpSub> memberMpSubs;
        private MemberHeat memberHeats;
    }

    @Data
    static class MemberSex{
        private Integer memberSex;
        private Integer sexCount;
    }

    @Data
    static class MemberChannel{
        private Integer memberChannel;
        private Integer channelCount;
    }

    @Data
    static class MemberMpSub{
        private Integer subCount;
        private Integer unSubCount;
    }


    @Data
    static class MemberHeat{
        private Integer reg;    // 只注册，未填写手机号
        private Integer complete;    // 完善了信息，填了手机号
        private Integer order;    // 下过订单
        private Integer orderAgain;    // 多次下单，复购
        private Integer coupon;    // 购买过优惠券，储值
    }



}
