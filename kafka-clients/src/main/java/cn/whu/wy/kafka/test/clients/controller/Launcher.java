package cn.whu.wy.kafka.test.clients.controller;

import cn.whu.wy.kafka.test.clients.service.OrderService;
import cn.whu.wy.kafka.test.clients.service.PayService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;

/**
 * @Author WangYong
 * @Date 2020/08/27
 * @Time 11:43
 */
@Component
public class Launcher implements InitializingBean {

    @Autowired
    private OrderService orderService;
    @Autowired
    private PayService payService;

    @Override
    public void afterPropertiesSet() throws Exception {
        Executors.newSingleThreadExecutor().execute(() -> orderService.receive());
        Executors.newSingleThreadExecutor().execute(()->payService.processPayReq());
        Executors.newSingleThreadExecutor().execute(()->orderService.sendReqToPay());

    }
}
