package pku.demo;

import pku.ByteMessage;
import pku.Consumer;
import pku.DefaultMessage;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class DemoConsumer extends Consumer {

    List<String> topics = new LinkedList<>();
    int readPos = 0;
    String queue;

    @Override
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
        if (queue != null) {
            throw new Exception("只允许绑定一次");
        }
        queue = queueName;
        topics.addAll(t);
    }

    @Override
    public ByteMessage poll() {
        ByteMessage re = null;
        //先读第一个topic, 再读第二个topic...
        //直到所有topic都读完了, 返回null, 表示无消息
        for (int i = 0; i < topics.size(); i++) {
            int index = (i + readPos) % topics.size();
            re = DemoMessageStore.store.pull(queue, topics.get(index));
            if (re != null) {
                readPos = index + 1;
                break;
            }
        }
        return re;
    }


}
