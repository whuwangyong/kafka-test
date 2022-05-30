package cn.whu.wy.kafka.test.clients.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * @author WangYong
 * Date 2020/08/27
 * Time 11:18
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PayRequest {
    private String orderId;
    private String userId;
    private BigDecimal amount;
    private LocalDateTime dateTime;
}
