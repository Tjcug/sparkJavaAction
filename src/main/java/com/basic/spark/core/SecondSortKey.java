package com.basic.spark.core;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 */
public class SecondSortKey implements Serializable,Ordered<SecondSortKey>{
    private int first;
    private int second;

    public SecondSortKey() {
    }

    public SecondSortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

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

    @Override
    public String toString() {
        return "SecondSortKey{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecondSortKey that = (SecondSortKey) o;

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
    public int compare(SecondSortKey that) {
        if(this.first- that.getFirst()!=0){
            return this.first-that.first;
        }else
            return this.second-that.second;
    }

    @Override
    public boolean $less(SecondSortKey that) {
        if(this.first<that.first){
            return true;
        }else if(this.first==that.first&&this.second<that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondSortKey that) {
        if(this.first>that.first){
            return true;
        }else if(this.first==that.first&&this.second>that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondSortKey that) {
        if(this.$less(that)){
            return true;
        }else if(this.first==that.first&&this.second==that.second){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondSortKey that) {
        if(this.$greater(that)){
            return true;
        }else if(this.first==that.first&&this.second==that.second){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SecondSortKey that) {
        if(this.first- that.getFirst()!=0){
            return this.first-that.first;
        }else
            return this.second-that.second;
    }
}
