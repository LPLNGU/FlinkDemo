package tablesql.bean;

/**
 * @Author: lipeilong
 * @Date: 2021/9/7 15:48
 */
public class Order {
    public String userId;
    public String product;
    public int amount;

    public Order() {
    }

    public Order(String userId, String product, int amount) {
        this.userId = userId;
        this.product = product;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + userId +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}
