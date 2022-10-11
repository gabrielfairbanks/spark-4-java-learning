package com.fairbanks;

import lombok.Data;


@Data
public class IntegerWithSquareRoot {

    private final int originalNumber;
    private final double squareRoot;


    public IntegerWithSquareRoot(int number) {
        this.originalNumber = number;
        this.squareRoot = Math.sqrt(number);
    }
}
