package cn.spark.core;

import scala.Serializable;
import scala.math.Ordered;

/**
 * 自定义二次排序key
 */
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable{
    //自定义key里定义需要排序的lie
    private int first;
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    //提供getter setter方法
    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    //提供hashcode equals

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondarySortKey that = (SecondarySortKey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    @Override
    public int compare(SecondarySortKey that) {
        if(this.first-that.first!=0){
            return this.first-that.first;
        }else{
            return this.second-that.second;
        }
    }

    @Override
    public int compareTo(SecondarySortKey that) {
        if(this.first-that.first!=0){
            return this.first-that.first;
        }else{
            return this.second-that.second;
        }
    }

    @Override
    public boolean $less(SecondarySortKey that) {
        if(this.first<that.first)
            return true;
        else if(this.first==that.first && this.second<that.second)
            return true;
        else
            return false;
    }

    @Override
    public boolean $greater(SecondarySortKey that) {
        if(this.first>that.first)
            return true;
        else if(this.first==that.first && this.second>that.second)
            return true;
        else
            return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey that) {
       if(that.$less(that)){
           return true;
       }else if(this.first==that.first && this.second==that.second){
           return true;
       }else
           return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey that) {
        if(that.$greater(that)){
            return true;
        }else if(this.first==that.first && this.second==that.second){
            return true;
        }else
            return false;
    }

}
