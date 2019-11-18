package cn.spark.study.streaming;

import org.apache.spark.sql.sources.In;

import java.io.Serializable;

public class ProductClickLog implements Serializable{
    private String category;
    private String product;
    private Integer count;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public ProductClickLog() {
    }

    public ProductClickLog(String category, String product, Integer count) {

        this.category = category;
        this.product = product;
        this.count = count;
    }
}
