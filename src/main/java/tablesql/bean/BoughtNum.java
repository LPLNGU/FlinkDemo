package tablesql.bean;

import java.util.Objects;

/**
 * @program: WordCountDemo
 * @description: 购买数量
 * @author: 李沛隆21081020
 * @create: 2021-11-26 14:55
 */
public class BoughtNum {
    public Long user;
    public int amount;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoughtNum boughtNum = (BoughtNum) o;
        return amount == boughtNum.amount && Objects.equals(user, boughtNum.user);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, amount);
    }

    public Long getUser() {
        return user;
    }

    public void setUser(Long user) {
        this.user = user;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public BoughtNum(Long user, int amount) {
        this.user = user;
        this.amount = amount;
    }
}
