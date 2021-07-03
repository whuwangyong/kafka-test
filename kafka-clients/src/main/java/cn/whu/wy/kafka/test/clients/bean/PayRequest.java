package cn.whu.wy.kafka.test.clients.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @Author WangYong
 * @Date 2020/08/27
 * @Time 11:18
 */
@Data
@Builder
public class PayRequest {
    private String orderId;
    private String userId;
    private BigDecimal amount;
    private LocalDateTime dateTime;
}
