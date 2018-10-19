package com.example.demo.elasticsearch;

import lombok.Data;

import java.util.Date;

@Data
public class EsModel {
    private String Id;
    private String Name;
    private Integer Age;
    private Date   Date;
}
