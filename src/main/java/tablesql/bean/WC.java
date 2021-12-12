package tablesql.bean;

/**
 * @Author: lipeilong
 * @Date: 2021/8/30 14:36
 */
public class WC {

    public String word;
    public Long num;

    public WC() {
    }

    public WC(String word, Long num) {
        this.word = word;
        this.num = num;
    }


    @Override
    public String toString() {
        return "WC{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }
}
