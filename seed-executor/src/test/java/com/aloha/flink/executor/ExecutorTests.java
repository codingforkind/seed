package com.aloha.flink.executor;

import com.aloha.flink.executor.exec.Executor;
import com.aloha.flink.executor.context.session.Session;
import org.junit.Test;

public class ExecutorTests {


    @Test
    public void tableRegister() throws Exception {
        Executor executor = new Executor();
        Session session = new Session();
        session.setToken("this-is-a-test-token");
        session.setTblSrcTopic("test-tblSrcTopic");
        executor.register(session);
    }

}
