package com.aloha.flink.executor.factory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFactory {

    public static Properties genKfkProperties() {
        InputStream in =
                PropertyFactory.class.getClassLoader().getResourceAsStream("kafka.properties");

        Properties kfkProps = new Properties();
        try {
            kfkProps.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return kfkProps;
    }

}
