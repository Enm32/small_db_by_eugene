package com.tinydb.server.storage_engine.common.file;


import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@ToString
@AllArgsConstructor
public class BlockId {
  private final String fileName;
  private final int blockNumber;
}
