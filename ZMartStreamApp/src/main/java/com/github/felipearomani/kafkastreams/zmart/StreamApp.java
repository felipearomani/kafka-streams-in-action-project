package com.github.felipearomani.kafkastreams.zmart;

import com.github.felipearomani.kafkastreams.zmart.clients.MockDataProducer;
import com.github.felipearomani.kafkastreams.zmart.model.Purchase;
import com.github.felipearomani.kafkastreams.zmart.model.PurchasePattern;
import com.github.felipearomani.kafkastreams.zmart.model.RewardAccumulator;
import com.github.felipearomani.kafkastreams.zmart.service.SecurityDBService;
import com.github.felipearomani.kafkastreams.zmart.util.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamApp.class);

    public static void main(String[] args) throws InterruptedException {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //1 - MASK CREDIT CARD
        KStream<String, Purchase> purchaseKStream = streamsBuilder
                .stream("transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());


        purchaseKStream.print(Printed.<String, Purchase>toSysOut().withLabel("PURCHASE"));

        //2 - EXTRACT PATTERN
        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        //3 - GET REWARD ACCUMULATOR
        KStream<String, RewardAccumulator> rewardAccumulatorKStream = purchaseKStream
                .mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardAccumulatorKStream.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        //4 - SYNC PURCHASES
//        purchaseKStream.to("purchases", Produced.with(stringSerde, purchaseSerde));

        // 5 - FILTER PURCHASE AND SELECT A KEY
        KStream<Long, Purchase> filteredKStream = purchaseKStream
                .filter((key, purchase) -> purchase.getPrice() > 5.00)
                .selectKey((key, purchase) -> purchase.getPurchaseDate().getTime());

        filteredKStream.print(Printed.<Long, Purchase>toSysOut().withLabel("FILTERED-PURCHASE"));

        filteredKStream.to("purchases", Produced.with(Serdes.Long(), StreamsSerdes.PurchaseSerde()));

        // 6 - BRANCH TO ELECTRONIC AND COFFEE DEPARTMENT
        KStream<String, Purchase>[] kStreamsByDept = purchaseKStream.branch(
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee"),
                (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics")
        );

        int coffee = 0;
        int electronics = 1;

        kStreamsByDept[coffee].to("coffee", Produced.with(stringSerde, purchaseSerde));
        kStreamsByDept[coffee].print(Printed.<String, Purchase>toSysOut().withLabel("COFFEE"));

        kStreamsByDept[electronics].to("electronics", Produced.with(stringSerde, purchaseSerde));
        kStreamsByDept[electronics].print(Printed.<String, Purchase>toSysOut().withLabel("ELECTRONIC"));

        // 7 - SAVE INFORMATION TO SECURITY TEAM

        ForeachAction<String, Purchase> foreachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(), purchase.getEmployeeId(), purchase.getItemPurchased());

        purchaseKStream
                .filter((key, purchase) -> purchase.getEmployeeId().equals("000000"))
                .foreach(foreachAction);


        // MOCK DATA FOR TEST PURPOSES
        MockDataProducer.producePurchaseData();


        // CREATE THE STREAM
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        LOGGER.info("ZMart Advanced Requirements Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOGGER.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();


    }


    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "FirstZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-purchase");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "FirstZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
