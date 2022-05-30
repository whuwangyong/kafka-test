package cn.whu.wy.kafka.test.clients.seek;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * Author WangYong
 * Date 2022/05/30
 * Time 12:31
 */
public class CheckAssign implements ConsumerRebalanceListener {

    private volatile boolean assigned = false;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("onPartitionsRevoked:" + partitions);
        assigned = false;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("onPartitionsAssigned:" + partitions);
        assigned = true;
    }

    public boolean isAssigned() {
        return assigned;
    }
}
