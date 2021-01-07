/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import org.h2.index.Cursor;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for a tree index.
 */
public class TreeCursor implements Cursor {
    private final TreeIndex tree;
    private TreeNode node;
    private boolean beforeFirst;
    private final SearchRow first, last;

    TreeCursor(TreeIndex tree, TreeNode node, SearchRow first, SearchRow last) {
        this.tree = tree;
        this.node = node;
        this.first = first;
        this.last = last;
        beforeFirst = true;
    }

    @Override
    public Row get() {
        return node == null ? null : node.row;
    }//返回row

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        if (beforeFirst) {
            beforeFirst = false;
            if (node == null) {
                return false;
            }
            if (first != null && tree.compareRows(node.row, first) < 0) {
                node = next(node);
            }
        } else {
            node = next(node);
        }
        if (node != null && last != null) {
            if (tree.compareRows(node.row, last) > 0) {
                node = null;
            }
        }
        return node != null;
    }

    @Override
    public boolean previous() {
        node = previous(node);
        return node != null;
    }

    /**
     * Get the next node if there is one.
     *
     * @param x the node
     * @return the next node or null
     */
    private static TreeNode next(TreeNode x) {//左中右的次序
        if (x == null) {
            return null;
        }
        TreeNode r = x.right;
        if (r != null) {//如果有右节点
            x = r;
            TreeNode l = x.left;
            while (l != null) {//一直找左节点
                x = l;
                l = x.left;
            }
            return x;//如果没有左节点，就直接返回右节点也可以
        }
        TreeNode ch = x;
        x = x.parent;
        while (x != null && ch == x.right) {//沿着右节点一直往上，知道不是右节点为止
            ch = x;
            x = x.parent;
        }
        return x;
    }


    /**
     * Get the previous node if there is one.
     *
     * @param x the node
     * @return the previous node or null
     */
    private static TreeNode previous(TreeNode x) {
        if (x == null) {
            return null;
        }
        TreeNode l = x.left;
        if (l != null) {//如果有有左节点，
            x = l;
            TreeNode r = x.right;
            while (r != null) {//一直找右节点
                x = r;
                r = x.right;
            }
            return x;//如果没有就直接返回左节点
        }
        TreeNode ch = x;
        x = x.parent;//父节点
        while (x != null && ch == x.left) {//沿着左节点一直向上，知道不是左节点为止
            ch = x;
            x = x.parent;
        }
        return x;
    }

}
