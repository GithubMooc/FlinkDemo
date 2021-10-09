package pojo;

import lombok.*;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 20:02
 * @Name 二叉树
 */
@NoArgsConstructor
@AllArgsConstructor
public class TreeNode {
    public int val;
    public TreeNode left;
    public TreeNode right;

    public TreeNode(int val) {
        this.val = val;
    }
}
