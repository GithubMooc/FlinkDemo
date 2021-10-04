package day06;

import lombok.*;

/**
 * @Author Master
 * @Date 2021/9/26
 * @Time 17:03
 * @Name FlinkDemo
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class Event {
    private String orderId;
    private String eventType;
    private Long timestamp;
}