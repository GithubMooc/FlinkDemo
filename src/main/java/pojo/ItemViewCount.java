package pojo;

import lombok.*;

import java.sql.Timestamp;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 21:29
 * @Name FlinkDemo
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
//每个商品在每个窗口中的浏览次数
public class ItemViewCount {
    private String itemId;
    private Long count;
    private Long windowStart;
    private Long windowEnd;

    @Override
    public String toString() {
        return "ItemViewCount{" +
                "itemId='" + itemId + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
