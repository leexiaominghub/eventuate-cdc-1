package io.eventuate.local.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.deserialization.WriteRowsEventDataDeserializer;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import io.eventuate.local.db.log.common.DbLogMetrics;

import java.io.IOException;
import java.io.Serializable;
import java.util.BitSet;

public class WriteRowsDeserializer extends WriteRowsEventDataDeserializer {

  private DbLogMetrics dbLogMetrics;

  public WriteRowsDeserializer(TableMapper tableMapper, DbLogMetrics dbLogMetrics) {
    super(tableMapper.getMappings());
    this.dbLogMetrics = dbLogMetrics;
  }

  @Override
  protected Serializable[] deserializeRow(long tableId, BitSet includedColumns, ByteArrayInputStream inputStream) throws IOException {
    dbLogMetrics.onBinlogEntryProcessed();
    return super.deserializeRow(tableId, includedColumns, inputStream);
  }
}
