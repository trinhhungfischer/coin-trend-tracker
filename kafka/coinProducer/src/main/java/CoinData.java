import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class CoinData implements Serializable {

    private String name;

    private ArrayList<String> symbol;

    private long price;
    private String description;

    public CoinData() {
        symbol = new ArrayList();
        symbol.add("BTC");
    }

    public static void main(String[] args) {
        System.out.println("Hello World!");

        CoinData coinData = new CoinData();
    }


}
