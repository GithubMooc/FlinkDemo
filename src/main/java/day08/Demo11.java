package day08;

import pojo.*;

/**
 * @Author Master
 * @Date 2021/10/9
 * @Time 19:51
 * @Name 链表与二叉树
 */
public class Demo11 {
    public static void main(String[] args) {

//        ListNode node3 = new ListNode(34, null);
//        ListNode node2 = new ListNode(97, node3);
//        ListNode node1 = new ListNode(12, node2);
        ListNode node1=new ListNode(12);
        node1.next=new ListNode(97);
        node1.next.next=new ListNode(34);
        ListNode node = node1;
        while (node != null) {
            System.out.println(node.val);
            node = node.next;
        }
        System.out.println("----------------------------");
        TreeNode root=new TreeNode(5);
        root.left=new TreeNode(3);
        root.right=new TreeNode(6);
        root.left.left=new TreeNode(1);
        root.left.right=new TreeNode(4);

       preOrderTraversal(root);
        System.out.println("--------------------------------");
        inOrderTraversal(root);
        System.out.println("----------------------------");
        postOrderTraversal(root);
        System.out.println("----------------------------");
        System.out.println(treeSearch(root, 2));
        System.out.println("----------------------------");

//        构建环形图
        GraphNode nodeA = new GraphNode(1);
        GraphNode nodeB = new GraphNode(2);
        GraphNode nodeC = new GraphNode(3);

        nodeA.nabour.add(nodeB);
        nodeB.nabour.add(nodeC);
        nodeC.nabour.add(nodeA);

    }
//    先序遍历
//    1、遍历根节点；2、对左子树进行先序遍历；3、对右子树进行先序遍历
    public static void preOrderTraversal(TreeNode root){
        if(root!=null){
            System.out.println(root.val);
            preOrderTraversal(root.left);
            preOrderTraversal(root.right);
        }
    }
//    中序遍历
    public static void inOrderTraversal(TreeNode root){
        if(root!=null){
            inOrderTraversal(root.left);
            System.out.println(root.val);
            inOrderTraversal(root.right);
        }
    }
//    后序遍历
    public static void postOrderTraversal(TreeNode root){
        if(root!=null){
            postOrderTraversal(root.left);
            postOrderTraversal(root.right);
            System.out.println(root.val);
        }
    }
//    有空指针异常问题
    public static boolean treeSearch(TreeNode root,int val){
        if(root.val==val){
            return true;
        }
        if(root.val<val){
            return treeSearch(root.right,val);
        }
        if(root.val>val){
            return treeSearch(root.left,val);
        }
        return false;
    }
}
