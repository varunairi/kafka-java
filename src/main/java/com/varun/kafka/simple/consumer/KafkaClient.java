package com.varun.kafka.simple.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Demo project.<p>
 *     Run with following configuration.
 *     <ul>
 *         <li>1 Zookeeper instance</li>
 *         <li>1 broker</li>
 *         <li> 1 Topic , 2 partitions</li>
 *     </ul>
 *     <br>
 *         Then this program starts 3 Consumer, out of which 2 of them will take 1 partiion each. 3rd will be idle.
 *         After 30 seconds, the threads 2 and 3 are killed, keeping only 1 consumer.
 *         At that point, Kafka reabalances, by allocating both partitions to 1 remaining consumer.
 *         This is because it misses the heartbeat from threads 2 and 3 , so it assumes only thread 1 is remaining and allocates both
 *         partitions to it.
 *
 *         <b> During the subsequent Reruns of the entire program, Kafka remembers where was the last offset , so next time this
 *         program runs, the offset does not begin from ZERO, but from the last offset where it left off. </b>
 *         This case deals with what happens when all consumers die OR are Recycled. So NO Messages are missed if all consumers are down.
 *
 *      <br>
 *          <b>If 1 broker goes down, all partitions on it are down (if replicas are not set up)</b>
 *
 * </p>
 * Run with following configuration
 * 1
 */
public class KafkaClient {


    public static void main(String[] args) throws InterruptedException {
        ExecutorService es= Executors.newFixedThreadPool(3);
        List<ConsumerPool> l = new ArrayList<>(3);
        for (int i = 0; i< 3; i ++){
            ConsumerPool c = new ConsumerPool();
            l.add(c);
            es.submit(c);
        }
//        Thread.sleep(30000);
//        System.out.println("Shutting down 2 and 3");
//        l.get(1).shutdown();
//        l.get(2).shutdown();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                for(ConsumerPool c: l){
                    c.shutdown();
                }
                es.shutdown();
                try {
                    es.awaitTermination(1000, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    private static class ConsumerPool implements Runnable {
        Properties props = new Properties();
        KafkaConsumer consumer ;
        public ConsumerPool(){
            props.put("key.deserializer", StringDeserializer.class.getName());
            props.put("value.deserializer", StringDeserializer.class.getName());
            props.put("group.id","ID1Test");
            props.put("bootstrap.servers", "localhost:9092");
        }

        public void run(){
            try {
                consumer = new KafkaConsumer(props);
                consumer.subscribe(Arrays.asList(new String[]{"myTopic"}));

                while (true) {
                    //consumer.poll is sending heartbeats, so all code below it must have shorter execution time
                    // than the session timeout (heartbeat duration).
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(60));
                    System.out.println(Thread.currentThread().getName() + ". Found Records:  " + consumerRecords.count());
                    for (ConsumerRecord<String, String> cr : consumerRecords) {
                        System.out.println(Thread.currentThread().getName() +
                                " " + cr.partition() + " " + cr.offset() + " ->" + cr.key() + ":" + cr.value());
                    }

                }
            }finally {
                consumer.close();
            }
        }
        public void shutdown()
        {
            consumer.wakeup();
        }
    }
}


