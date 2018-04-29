package ai.fma.mpi_yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
    static Logger logger = LoggerFactory.getLogger(Test.class);
    public static void main(String[] args)
    {
        logger.info("HELLO, WORLD");
        for(int i=0; i<10; i++) {
            logger.info(String.valueOf(i));
        }
    }
}
