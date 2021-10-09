package pojo;

import lombok.*;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 19:53
 * @Name 链表数据结构定义
 */
@NoArgsConstructor
@AllArgsConstructor
public class ListNode {
    public int val;
    public ListNode next;

    public ListNode(int val) {
        this.val = val;
    }
}
