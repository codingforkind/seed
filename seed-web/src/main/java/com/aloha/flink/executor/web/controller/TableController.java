package com.aloha.flink.executor.web.controller;

import com.alibaba.fastjson.JSON;
import com.aloha.flink.common.protocol.Tbl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;

@Slf4j
@RestController
@RequestMapping("/mq/table")
public class TableController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @PostMapping("/init")
    public void init(Tbl tbl) {

        log.info("got a tbl init msg.....");

        kafkaTemplate.send("test-tblSrcTopic", JSON.toJSONString(initTbl(tbl)));

    }

    private Tbl initTbl(Tbl tbl) {

        tbl = new Tbl();
        tbl.setSchema("test");
        tbl.setTblName("test");
        tbl.setFiledNameList(Arrays.asList("id", "name"));
        tbl.setFiledTypeList(Arrays.asList("Integer", "String"));
        return tbl;
    }
}
