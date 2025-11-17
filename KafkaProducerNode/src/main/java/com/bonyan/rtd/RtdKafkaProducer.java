package com.bonyan.rtd;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

import com.comptel.eventlink.core.Nodebase;
import com.comptel.mc.node.*;
import com.comptel.mc.node.lookup.*;
import com.comptel.mc.node.logging.NodeLoggerFactory;
import com.comptel.mc.node.logging.TxeLogger;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class KafkaProducer extends Nodebase implements BusinessLogic, Schedulable, TimerObserver, LookupServiceUser {

    private static final Logger logger = Logger.getLogger(KafkaProducer.class.getName());
    private static final TxeLogger nodeLogger = NodeLoggerFactory.getNodeLogger(KafkaProducer.class.getCanonicalName());


    @Override
    public void setLookupService(LookupService service) {

    }

    @Override
    public void init(NodeContext nodeContext) throws Exception {
        String topicName = "test-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.197:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // ensure message durability

    }

    @Override
    public void flush() throws Exception {
        nodeLogger.info("nodeLogger: node flush start");
    }

    @Override
    public void end() throws Exception {
        nodeLogger.info("nodeLogger: node 'end' start");
    }

    @Override
    public void request(String s) throws Exception {
        nodeLogger.info("nodeLogger: node request start, input String: " + s);
    }

    @Override
    public void pause(int i) throws Exception {
        nodeLogger.info("nodeLogger: pause method, input int: " + i);
    }

    @Override
    public void resume(int i) throws Exception {
        nodeLogger.info("nodeLogger: resume method, input int: " + i);

    }

    @Override
    public void process(EventRecord eventRecord) throws Exception {

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                String message = "Hello Kafka " + i;
                producer.send(new ProducerRecord<>(topicName, Integer.toString(i), message));
                System.out.println("Sent: " + message);
            }
        }
    }

    @Override
    public void setService(EventRecordService eventRecordService) {


    }

    @Override
    public void schedule() throws Exception {

    }

    @Override
    public void timer() throws Exception {

    }


}
