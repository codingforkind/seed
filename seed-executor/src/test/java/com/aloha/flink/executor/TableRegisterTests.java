package com.aloha.flink.executor;

import com.aloha.flink.executor.exec.TableRegister;
import com.aloha.flink.executor.session.Session;
import org.junit.Test;

public class TableRegisterTests {


    @Test
    public void tableRegister() throws Exception {
        TableRegister tableRegister = new TableRegister();
        Session session = new Session();
        session.setToken("this-is-a-test-token");
        session.setTopic("test-topic");
        tableRegister.register(session);
    }

}
