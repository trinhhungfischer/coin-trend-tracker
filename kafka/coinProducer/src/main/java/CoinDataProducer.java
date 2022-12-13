
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

public class CoinDataProducer {

    private static final Logger logger = Logger.getLogger(CoinDataProducer.class);

    private final Producer<String, CoinData> producer;

    public CoinDataProducer(final Producer<String, CoinData> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws Exception {
        Properties properties = PropertyFileReader.readPropertyFile();

        Producer<String, CoinData> producer = new Producer(new ProducerConfig(properties));
        CoinDataProducer coinProducer = new CoinDataProducer(producer);

    }


    private void generateCoinData(String topic) throws InterruptedException {

    }

    


}
