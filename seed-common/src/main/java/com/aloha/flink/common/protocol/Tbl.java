package com.aloha.flink.common.protocol;

import lombok.Data;

import java.util.List;

@Data
public class Tbl {

    private String schema;
    private String tblName;
    private List<String> filedNameList;
    private List<String> filedTypeList;

}
