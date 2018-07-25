package org.apache.lucene.util.keyvi;

public class KeyviConstants {
  public static final int MAX_TRANSITIONS_OF_A_STATE = 257;
  public static final int FINAL_OFFSET_TRANSITION = 256;
  public static final int INNER_WEIGHT_TRANSITION_COMPACT = 260;
  public static final char FINAL_OFFSET_CODE = 1;
  public static final int NUMBER_OF_STATE_CODINGS = 255;
  public static final long SPARSE_ARRAY_SEARCH_OFFSET = 151;
  public static final int COMPACT_SIZE_RELATIVE_MAX_VALUE = 32768;
  public static final int COMPACT_SIZE_ABSOLUTE_MAX_VALUE = 16384;
  public static final int COMPACT_SIZE_WINDOW = 512;
  public static final int COMPACT_SIZE_INNER_WEIGHT_MAX_VALUE = 0xffff;
  public static final byte[] KEYVI_FILE_MAGIC = {'K', 'E', 'Y', 'V', 'I', 'F', 'S', 'A'};
}
