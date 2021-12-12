package tablesql.bean;

import scala.math.BigInt;

import java.util.Objects;

public class TestTableRes {
    public BigInt user_id;
    public BigInt item_id;
    public BigInt category_id;
    public String behavior;
    public String ts;

    public TestTableRes() {

    }

    public TestTableRes(BigInt userId, BigInt item_id, BigInt category_id, String behavior, String ts) {
        this.user_id = userId;
        this.item_id = item_id;
        this.category_id = category_id;
        this.behavior = behavior;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "TestTableRes{" +
                "userId=" + user_id +
                ", itemId=" + item_id +
                ", categoryId=" + category_id +
                ", behavior='" + behavior + '\'' +
                ", timeStamp=" + ts +
                '}';
    }
}
