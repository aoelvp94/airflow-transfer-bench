package dev.transferbench;

import java.util.List;

import org.apache.airflow.sdk.Bundle;
import org.apache.airflow.sdk.Server;

/** Entry point the JavaCoordinator launches; serves the task handler for the Python stub. */
public final class Main {
  public static void main(String[] args) {
    Server.create(args).serve(new Bundle(List.of()).register("transfer_bench", "transfer_java", TransferJava.class));
  }
}
