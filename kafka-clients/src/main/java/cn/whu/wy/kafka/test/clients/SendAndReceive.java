package cn.whu.wy.kafka.test.clients;

import cn.whu.wy.kafka.test.clients.service.OrderService;
import cn.whu.wy.kafka.test.clients.service.PayService;

/**
 * @Author WangYong
 * @Date 2021/07/03
 * @Time 21:14
 */
public class SendAndReceive {
    public static void main(String[] args) {

        OrderService orderService = new OrderService();
        PayService payService = new PayService();

        orderService.sendReqToPay();
        new Thread(orderService::receive).start();
        new Thread(payService::processPayReq).start();
    }
}
