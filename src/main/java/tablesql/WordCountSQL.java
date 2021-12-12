package tablesql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import tablesql.bean.WC;

/**
 * @Author: lipeilong
 * @Date: 2021/9/7 14:56
 */
public class WordCountSQL {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tenv = BatchTableEnvironment.create(env);

        DataSet<WC> wcDataSet = env.fromElements(new WC("hello", (long) 10),
                new WC("world", (long) 2),
                new WC("world", (long) 4),
                new WC("hello", (long) 1));

        Table table = tenv.fromDataSet(wcDataSet);
        tenv.createTemporaryView("wc", table);

        table.printSchema();

        Table sqlQuery = tenv.sqlQuery("select word,sum(num) as num from wc group by word");



        DataSet<WC> resDataSet = tenv.toDataSet(sqlQuery, WC.class);
        resDataSet.print();

    }
}
