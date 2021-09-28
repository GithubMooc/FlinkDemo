package pojo;

import lombok.*;

/**
 * @Author Master
 * @Date 2021/9/28
 * @Time 21:17
 * @Name FlinkDemo
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserBehavior {
    private String userId;
    private String itemId;
    private String categoryId;
    private String behavior;
    private Long timestamp;
}
