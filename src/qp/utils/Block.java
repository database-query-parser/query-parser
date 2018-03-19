package qp.utils;

import java.util.Vector;
import java.io.Serializable;

public class Block implements Serializable {

    int MAX_SIZE; // number of pages per block
    static int BlockSize; // number of pages per block

    Vector pages; // the pages in the block

    /** Set number of pages per block **/
    public static void setBlockSize(int size){
        BlockSize=size;
    }

    /** get number of pages per block **/
    public static int getBlockSize(){
        return BlockSize;
    }

    public Block(int numPages) {
        this.MAX_SIZE = numPages;
        this.pages = new Vector(MAX_SIZE);
    }

    public void add(Batch b) {
        pages.add(b);
    }

    public int capacity() {
        return MAX_SIZE;
    }

    public void clear(){
        pages.clear();
    }

    public boolean contains(Batch b){
        return pages.contains(b);
    }

    public Batch elementAt(int i){
        return (Batch) pages.elementAt(i);
    }

    public int indexOf(Batch b){
        return pages.indexOf(b);
    }

    public void insertElementAt(Batch b, int i){
        pages.insertElementAt(b,i);
    }

    public boolean isEmpty(){
        return pages.isEmpty();
    }

    public void remove(int i){
        pages.remove(i);
    }

    public void setElementAt(Batch b, int i){
        pages.setElementAt(b,i);
    }

    public int size(){
        return pages.size();
    }

    public boolean isFull(){
        if(size() == capacity())
            return true;
        else
            return false;
    }
}
