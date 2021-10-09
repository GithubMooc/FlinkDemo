package pojo;

import lombok.*;

import java.util.*;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 19:53
 * @Name 链表数据结构定义
 */
@NoArgsConstructor
@AllArgsConstructor
public class GraphNode {
    public int val;
    public List<GraphNode> nabour=new ArrayList<>();

    public GraphNode(int val) {
        this.val = val;
    }
}
