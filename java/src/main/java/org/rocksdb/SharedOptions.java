package org.rocksdb;


public class SharedOptions extends RocksObject {
  public SharedOptions(
      final long total_ram_size_bytes, final long total_threads, final long delayed_write_rate) {
    super(newSharedOptions(total_ram_size_bytes, total_threads, delayed_write_rate));
  }

  private native static long newSharedOptions(
      long total_ram_size_bytes, final long total_threads, final long delayed_write_rate);

  @Override
  protected native void disposeInternal(final long handle);

}
