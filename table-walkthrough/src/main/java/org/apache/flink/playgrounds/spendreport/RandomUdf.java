package org.apache.flink.playgrounds.spendreport;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.flink.table.annotation.DataTypeHint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.functions.ScalarFunction;


public class RandomUdf extends ScalarFunction {
  private static final Logger LOG = LoggerFactory.getLogger(IntInputUdf.class);

  public @DataTypeHint("Bytes") byte[] eval(@DataTypeHint("INT") Integer intputNum) {
    byte[] results = intputNum.toString().getBytes(StandardCharsets.UTF_8);
    int randomNumber = ((int) (Math.random() * (10 - 1))) + 1;
    LOG.info("[*][*][*] input num is {} and random number is {}. [*][*][*]", intputNum, randomNumber);
    if (randomNumber % 2 == 0) {
      LOG.info("### ### input bytes {} and num {}.   ### ### DEBUG ### ### duplicated call??? ### DEBUG  ### ### ", results, intputNum);
      return results;
    }
    LOG.info("*** *** input bytes {} and num {}.", results, intputNum);
    return null;
  }

}