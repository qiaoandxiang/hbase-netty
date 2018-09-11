package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public interface CompactedTypeHelper<K, V> {
  
  @InterfaceAudience.Private
  static public class KVPair<K, V> {
    public KVPair() {
    }

    public KVPair(K k, V v) {
      this.key = k;
      this.value = v;
    }

    public K key;
    public V value;
  }

  /**
   * Get estimated size of compacted k-v
   * @param key
   * @param value
   * @return estimated size
   */
  int getCompactedSize(K key, V value);
  
  /**
   * Compact the key&value into data.
   * ONLY the area from data[offset] to data[offset+len-1] can be used for this key&value
   * If you write out the area, the result is unpredictable
   *  
   * @param key
   * @param value
   * @param data
   * @param offset
   * @param len
   */
  void compact(K key, V value, byte[] data, int offset, int len);
  
  /**
   * Deserialize the key&value from data
   * ONLY the area from data[offset] to data[offset+len-1] can be used for this key&value
   * If you write out the area, the result is unpredictable
   * 
   * @param data
   * @param offset
   * @param len
   * @return
   */
  KVPair<K, V> decomposte(byte[] data, int offset, int len);
  
  /**
   * Compare kv1 at data1[offset1, offset1+len1) with kv2 at data2[offset2, offset2+len2)
   * if key1 == key2:
   *     return 0
   * if key1 > key2:
   *     return >0
   * if key1 < key2:
   *     return <0
   * @param data1  kv1's data fragment
   * @param offset1 kv1's data offset in the fragment
   * @param len1  kv1's data length in the fragment
   * @param data2 kv2's data fragment
   * @param offset TODO
   * @param len2
   * @return
   */
  int compare(byte[] data1, int offset1, int len1, byte[] data2, int offset, int len2);
  int compare(K key, byte[] data, int offset, int len);
  int compare(K key1, K key2); 
}
