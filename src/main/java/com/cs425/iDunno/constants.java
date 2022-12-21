package com.cs425.iDunno;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class constants {
    public static final List<String> train_api = new ArrayList<String>(){{
        add("http://localhost:8080/resnet/train");
        add("http://localhost:8080/inception/train");
    }};
    public static final String BASE_API = "http://localhost:8080/";
    public static final String BASE_PATH = "/home/jadonts2/model/test/data/";
//    public static final String BASE_PATH = "/Users/suzy/Desktop/mp4_data/model/test/data/";
    public static final String SDFS_LOCAL_PATH = "images_";
    public static final String RESULT_BASE_PATH = "/home/jadonts2/model/result/";
//    public static final String RESULT_BASE_PATH = "/Users/suzy/Desktop/mp4_data/model/result/";
    public static final HashSet<String> models = new HashSet<String>(){{
        add("resnet");
        add("inception");
    }};
}
