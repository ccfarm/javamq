package pku.demo;

import pku.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * Created by yangxiao on 2017/11/14.
 * 这个程序演示了测评程序的基本逻辑
 * 正式的测评程序会比这个更复杂
 */
public class DemoTester {
    //每个pusher向每个topic发送的消息数目
    static int PUSH_COUNT = 1000;
    //发送消息的线程数
    static int PUSH_THREAD_COUNT = 10;
    //发送线程往n个topic发消息
    static int PUSH_TOPIC_COUNT = 2;
    //消费消息的线程数
    static int PULL_THREAD_COUNT = 10;
    //每个消费者消费的topic数量
    static int PULL_TOPIC_COUNT = 4;
    //topic数量
    static int TOPIC_COUNT = 20;
    //每个queue绑定的topic数量
    static int ATTACH_COUNT = 2;

    //统计push/pull消息的数量
    static AtomicInteger pushCount = new AtomicInteger();
    static AtomicInteger pullCount = new AtomicInteger();

    static class PushTester implements Runnable {
        //随机向以下topic发送消息
        List<String> topics = new ArrayList<>();
        Producer producer = new DemoProducer();
        int id;

        PushTester(List<String> t, int id) {
            topics.addAll(t);
            this.id = id;
            StringBuilder sb=new StringBuilder();
            sb.append(String.format("producer%d push to:",id));
            for (int i = 0; i <t.size() ; i++) {
                sb.append(t.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < topics.size(); i++) {
                    String topic = topics.get(i);
                    for (int j = 0; j < PUSH_COUNT; j++) {
                        //topic加j作为数据部分
                        //j是序号, 在consumer中会用来校验顺序
                        byte[] data = (topic +" "+id + " " + j).getBytes();
                        ByteMessage msg = producer.createBytesMessageToTopic(topics.get(i), data);
                        //设置一个header
                        msg.putHeaders(MessageHeader.SEARCH_KEY, "hello");
                        //发送消息
                        producer.send(msg);
                        pushCount.incrementAndGet();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    static class PullTester implements Runnable {
        //拉消息
        String queue;
        List<String> topics = new ArrayList<>();
        Consumer consumer = new DemoConsumer();
        int pc=0;
        public PullTester(String s, ArrayList<String> tops) throws Exception {
            queue = s;
            topics.addAll(tops);
            consumer.attachQueue(s, tops);

            StringBuilder sb=new StringBuilder();
            sb.append(String.format("queue%s attach:",s));
            for (int i = 0; i <topics.size() ; i++) {
                sb.append(topics.get(i)+" ");
            }
            System.out.println(sb.toString());
        }

        @Override
        public void run() {

            try {
                //检查顺序, 保存每个topic-producer对应的序号, 新获得的序号必须严格+1
                HashMap<String, Integer> posTable = new HashMap<>();
                while (true) {
                    ByteMessage msg = consumer.poll();
                    if (msg == null) {
                        System.out.println(String.format("thread pull %s",pc));
                        return;
                    } else {
                        byte[] data = msg.getBody();
                        String str = new String(data);
                        String[] strs = str.split(" ");
                        String topic = strs[0];
                        String prod = strs[1];
                        int j = Integer.parseInt(strs[2]);
                        String mapkey=topic+" "+prod;
                        if (!posTable.containsKey(mapkey)) {
                            posTable.put(mapkey, 0);
                        }
                        if (j != posTable.get(mapkey)) {
                            System.out.println(String.format("数据错误 topic %s 序号:%d", topic, j));
                            System.exit(0);
                        }
                        if (!msg.headers().getString(MessageHeader.SEARCH_KEY).equals("hello")) {
                            System.out.println(String.format("header错误 topic %s 序号:%d", topic, j));
                            System.exit(0);
                        }
                        posTable.put(mapkey, posTable.get(mapkey) + 1);
                        pullCount.incrementAndGet();
                        pc++;
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }


    public static void main(String args[]) {
        try {
            //topic的名字是topic+序号的形式
            DemoMessageStore store = DemoMessageStore.store;
            System.out.println("开始push");
            long time1 = System.currentTimeMillis();
            Random rand = new Random(0);
            ArrayList<Thread> pushers = new ArrayList<>();
            for (int i = 0; i < PUSH_THREAD_COUNT; i++) {
                //随机选择连续的topic
                ArrayList<String> tops = new ArrayList<>();
                int start = rand.nextInt(TOPIC_COUNT);
                for (int j = 0; j < PUSH_TOPIC_COUNT; j++) {
                    int v = (start+j)%TOPIC_COUNT;
                    tops.add("topic"+Integer.toString(v));
                }
                Thread t = new Thread(new PushTester(tops, i));
                t.start();
                pushers.add(t);
            }
            for (int i = 0; i < pushers.size(); i++) {
                pushers.get(i).join();
            }
            long time2 = System.currentTimeMillis();
            System.out.println(String.format("push 结束 time cost %d push count %d", time2 - time1, pushCount.get()));
            System.out.println("开始pull");

            int queue = 0;
            ArrayList<Thread> pullers = new ArrayList<>();
            for (int i = 0; i < PULL_THREAD_COUNT; i++) {
                //随机选择topic
                ArrayList<String> tops = new ArrayList<>();
                int start = rand.nextInt(TOPIC_COUNT);
                for (int j = 0; j < PULL_TOPIC_COUNT; j++) {
                    int v =(start+j)%TOPIC_COUNT;
                    tops.add("topic"+Integer.toString(v));
                }
                Thread t = new Thread(new PullTester(Integer.toString(queue), tops));
                queue++;
                t.start();
                pullers.add(t);
            }
            for (int i = 0; i < pullers.size(); i++) {
                pullers.get(i).join();
            }
            long time3 = System.currentTimeMillis();
            System.out.println(String.format("pull 结束 time cost %d pull count %d", time3 - time2, pullCount.get()));


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
