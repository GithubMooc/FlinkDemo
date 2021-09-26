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
    public String user;
    public String url;
    public Long timestamp;
}