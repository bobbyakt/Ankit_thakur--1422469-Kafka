
package com.company.examples.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "pogId",
    "supc",
    "brand",
    "description",
    "size",
    "category",
    "subcategory",
    "price",
    "quantity",
    "country",
    "sellercode",
    "creationtime",
    "stock"
})
public class product {

    @JsonProperty("pogId")
    private String pogId;
    @JsonProperty("supc")
    private String supc;
    @JsonProperty("brand")
    private String brand;
    @JsonProperty("description")
    private String description;
    @JsonProperty("size")
    private String size;
    @JsonProperty("category")
    private String category;
    @JsonProperty("subcategory")
    private String subcategory;
    @JsonProperty("price")
    private String price;
    @JsonProperty("quantity")
    private String quantity;
    @JsonProperty("country")
    private String country;
    @JsonProperty("sellercode")
    private String sellercode;
    @JsonProperty("creationtime")
    private String creationtime;
    @JsonProperty("stock")
    private String stock;

    @JsonProperty("pogId")
    public String getPogId() {
        return pogId;
    }

    @JsonProperty("pogId")
    public void setPogId(String pogId) {
        this.pogId = pogId;
    }

    public product withPogId(String pogId) {
        this.pogId = pogId;
        return this;
    }

    @JsonProperty("supc")
    public String getSupc() {
        return supc;
    }

    @JsonProperty("supc")
    public void setSupc(String supc) {
        this.supc = supc;
    }

    public product withSupc(String supc) {
        this.supc = supc;
        return this;
    }

    @JsonProperty("brand")
    public String getBrand() {
        return brand;
    }

    @JsonProperty("brand")
    public void setBrand(String brand) {
        this.brand = brand;
    }

    public product withBrand(String brand) {
        this.brand = brand;
        return this;
    }

    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
    }

    public product withDescription(String description) {
        this.description = description;
        return this;
    }

    @JsonProperty("size")
    public String getSize() {
        return size;
    }

    @JsonProperty("size")
    public void setSize(String size) {
        this.size = size;
    }

    public product withSize(String size) {
        this.size = size;
        return this;
    }

    @JsonProperty("category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("category")
    public void setCategory(String category) {
        this.category = category;
    }

    public product withCategory(String category) {
        this.category = category;
        return this;
    }

    @JsonProperty("subcategory")
    public String getSubcategory() {
        return subcategory;
    }

    @JsonProperty("subcategory")
    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
    }

    public product withSubcategory(String subcategory) {
        this.subcategory = subcategory;
        return this;
    }

    @JsonProperty("price")
    public String getPrice() {
        return price;
    }

    @JsonProperty("price")
    public void setPrice(String price) {
        this.price = price;
    }

    public product withPrice(String price) {
        this.price = price;
        return this;
    }

    @JsonProperty("quantity")
    public String getQuantity() {
        return quantity;
    }

    @JsonProperty("quantity")
    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public product withQuantity(String quantity) {
        this.quantity = quantity;
        return this;
    }

    @JsonProperty("country")
    public String getCountry() {
        return country;
    }

    @JsonProperty("country")
    public void setCountry(String country) {
        this.country = country;
    }

    public product withCountry(String country) {
        this.country = country;
        return this;
    }

    @JsonProperty("sellercode")
    public String getSellercode() {
        return sellercode;
    }

    @JsonProperty("sellercode")
    public void setSellercode(String sellercode) {
        this.sellercode = sellercode;
    }

    public product withSellercode(String sellercode) {
        this.sellercode = sellercode;
        return this;
    }

    @JsonProperty("creationtime")
    public String getCreationtime() {
        return creationtime;
    }

    @JsonProperty("creationtime")
    public void setCreationtime(String creationtime) {
        this.creationtime = creationtime;
    }

    public product withCreationtime(String creationtime) {
        this.creationtime = creationtime;
        return this;
    }

    @JsonProperty("stock")
    public String getStock() {
        return stock;
    }

    @JsonProperty("stock")
    public void setStock(String stock) {
        this.stock = stock;
    }

    public product withStock(String stock) {
        this.stock = stock;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("pogId", pogId).append("supc", supc).append("brand", brand).append("description", description).append("size", size).append("category", category).append("subcategory", subcategory).append("price", price).append("quantity", quantity).append("country", country).append("sellercode", sellercode).append("creationtime", creationtime).append("stock", stock).toString();
    }

}
