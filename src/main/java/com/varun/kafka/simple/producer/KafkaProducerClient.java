package com.varun.kafka.simple.producer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <ul>
 * <li><a href="https://docs.confluent.io/current/clients/producer.html">Documentation on Producer</a></li>
 * <li><a href="https://docs.confluent.io/current/clients/java.html">Documentation on Java Client</a></li>
 * </ul>
 *  <br>
 *      Trying to publish on a topi with acknowledgemnts from all replicas of a partition. <i>Note: if one of the partitions is down
 *      then you will get a Timeout Exception from get() method.
 *      </i>
 *      If you are not interested in following up on whether message was produced successfully on Broker, just dont use get() method
 *      and move on with the loop.
 */
public class KafkaProducerClient {

    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, TimeoutException {
        Properties prop = new Properties();
        prop.put("key.serializer", IntegerSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("client.id", InetAddress.getLocalHost().getHostName()); //Optional, to recognize the producer on broker
        prop.put("acks", "1");//important: do you want ack from leader and replicas , if only leader then 1, and if none then 0 (at 0 you wont get back the offset)
        prop.put("compression.type","gzip");//optional

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(prop);
        for (int i = 0; i < 1000; i ++ ) {
            ProducerRecord record = new ProducerRecord<Integer, String>("myTopic", i, "Message  + " + i);
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata rm = future.get(1999, TimeUnit.MILLISECONDS);
            if(rm!=null)
                System.out.println("Producing i: " + i + " on " + rm.partition() + ":" + rm.offset());
        }
        producer.close();

    }
}
