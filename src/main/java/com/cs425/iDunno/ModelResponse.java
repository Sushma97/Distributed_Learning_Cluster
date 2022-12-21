package com.cs425.iDunno;

import java.util.List;

public class ModelResponse {
    private String message;
    private List<String> result;

    public ModelResponse() {
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<String> getResult() {
        return result;
    }

    public void setResult(List<String> result) {
        this.result = result;
    }
}