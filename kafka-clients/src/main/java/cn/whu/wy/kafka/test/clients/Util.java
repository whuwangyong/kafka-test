package cn.whu.wy.kafka.test.clients;

import com.google.gson.Gson;

/**
 * @Author WangYong
 * @Date 2021/07/03
 * @Time 20:52
 */
public class Util {

    public static final String PAY_REQ_TOPIC = "pay-request";
    public static final String PAY_RESP_TOPIC = "pay-response";

    public static Gson gson(){
        return new Gson();
    }
}
