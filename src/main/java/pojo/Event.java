package pojo;

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
    private String user;
    private String url;
    private Long timestamp;
}