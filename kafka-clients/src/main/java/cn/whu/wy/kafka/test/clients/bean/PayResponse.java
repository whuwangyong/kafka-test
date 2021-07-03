package cn.whu.wy.kafka.test.clients.bean;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * @Author WangYong
 * @Date 2020/08/27
 * @Time 11:24
 */
@Data
@Builder
public class PayResponse {
    private String orderId;
    private LocalDateTime dateTime;
    private String result;
}
