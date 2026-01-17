package com.tinydb.server.storage_engine.impl;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;


@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
public class RecordKey implements Serializable {

  private static final long serialVersionUID = 1L;


  private int blockNumber;
  private int slotNumber;
}
