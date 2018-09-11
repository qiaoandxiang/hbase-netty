/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.CompactedConcurrentSkipListMap;
import org.apache.hadoop.hbase.util.CompactedConcurrentSkipListMap.PutAndFetch;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * A {@link java.util.Set} of {@link Cell}s implemented on top of a
 * {@link java.util.concurrent.ConcurrentSkipListMap}.  Works like a
 * {@link java.util.concurrent.ConcurrentSkipListSet} in all but one regard:
 * An add will overwrite if already an entry for the added key.  In other words,
 * where CSLS does "Adds the specified element to this set if it is not already
 * present.", this implementation "Adds the specified element to this set EVEN
 * if it is already present overwriting what was there previous".  The call to
 * add returns true if no value in the backing map or false if there was an
 * entry with same key (though value may be different).
 * <p>Otherwise,
 * has same attributes as ConcurrentSkipListSet: e.g. tolerant of concurrent
 * get and set and won't throw ConcurrentModificationException when iterating.
 */
@InterfaceAudience.Private
public class CellSkipListSet implements NavigableSet<Cell> {
  private final static Log LOG = LogFactory.getLog(CellSkipListSet.class);
  private final ConcurrentNavigableMap<Cell, Cell> delegatee;
  private final MemStoreChunkPool chunkPool;
  private final MemStoreLAB allocator;
  private boolean isEmbededCCSMap = false;
  
  final private class PageAllocatorForCompactedMap implements
      CompactedConcurrentSkipListMap.PageAllocator {

    @Override
    public Object[] allocateHeapKVPage(int sz) {
      return new Object[sz];
    }

    @Override
    public byte[] allocatePages(int sz) {
      if (allocator != null) {
        ByteRange a = allocator.allocateBytes(sz);
        // The chunk is only used for pages, always get a clean new page once
        assert a.getOffset() == 0;
        assert a.getBytes().length >= sz;
        return a.getBytes();
      } else {
        return new byte[sz];
      }
    }
  }
  
  CellSkipListSet(Configuration conf, final KeyValue.KVComparator c) {
    if (conf.getBoolean(DefaultMemStore.USEMSLAB_KEY,
        DefaultMemStore.USEMSLAB_DEFAULT)) {
      this.chunkPool = MemStoreChunkPool.getPool(conf);
    } else {
      this.chunkPool = null;
    }
    isEmbededCCSMap = conf.getBoolean(DefaultMemStore.USECCSMAP_KEY,
        DefaultMemStore.USECCSMAP_DEFAULT);
    if (this.chunkPool != null) {
      if (isEmbededCCSMap) {
        this.allocator = new HeapMemStoreLAB(conf,
            this.chunkPool.getChunkSize());
      } else {
        String className = conf.get(DefaultMemStore.MSLAB_CLASS_NAME,
            HeapMemStoreLAB.class.getName());
        this.allocator = ReflectionUtils.instantiateWithCustomCtor(className,
            new Class[] { Configuration.class }, new Object[] { conf });
      }
    } else {
      this.allocator = null;
    }
    
    if (isEmbededCCSMap) {
      delegatee = createCompcatedSkipList(conf, c);
    } else {
      delegatee = new ConcurrentSkipListMap<Cell, Cell>(c);
    }
  }
  
  private ConcurrentNavigableMap<Cell, Cell> createCompcatedSkipList(
      Configuration conf, KeyValue.KVComparator c) {
    CompactedConcurrentSkipListMap.PageSetting ps = new CompactedConcurrentSkipListMap.PageSetting();

    int pageSize = chunkPool == null ? conf.getInt(
        HeapMemStoreLAB.CHUNK_SIZE_KEY, HeapMemStoreLAB.CHUNK_SIZE_DEFAULT)
        : this.chunkPool.getChunkSize();
    int threshold = conf.getInt(HeapMemStoreLAB.MAX_ALLOC_KEY,
        HeapMemStoreLAB.MAX_ALLOC_DEFAULT);

    ps.setDataPageSize(pageSize);
    ps.setHeapKVThreshold(threshold);
    ps.setPageAllocator(new PageAllocatorForCompactedMap());

    return new CompactedConcurrentSkipListMap<Cell, Cell>(
        new KeyValue.CompactedCellTypeHelper(c), ps);
  }

  CellSkipListSet(final KeyValue.KVComparator c) {
    this.delegatee = new ConcurrentSkipListMap<Cell, Cell>(c);
    this.chunkPool = null;
    this.allocator = null;
  }

  CellSkipListSet(final ConcurrentNavigableMap<Cell, Cell> m) {
    this.delegatee = m;
    this.chunkPool = null;
    this.allocator = null;
  }

  public Cell ceiling(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Iterator<Cell> descendingIterator() {
    return this.delegatee.descendingMap().values().iterator();
  }

  public NavigableSet<Cell> descendingSet() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell floor(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public SortedSet<Cell> headSet(final Cell toElement) {
    return headSet(toElement, false);
  }

  public NavigableSet<Cell> headSet(final Cell toElement,
      boolean inclusive) {
    return new CellSkipListSet(this.delegatee.headMap(toElement, inclusive));
  }

  public Cell higher(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Iterator<Cell> iterator() {
    return this.delegatee.values().iterator();
  }

  public Cell lower(Cell e) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell pollFirst() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell pollLast() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public SortedSet<Cell> subSet(Cell fromElement, Cell toElement) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public NavigableSet<Cell> subSet(Cell fromElement,
      boolean fromInclusive, Cell toElement, boolean toInclusive) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public SortedSet<Cell> tailSet(Cell fromElement) {
    return tailSet(fromElement, true);
  }

  public NavigableSet<Cell> tailSet(Cell fromElement, boolean inclusive) {
    return new CellSkipListSet(this.delegatee.tailMap(fromElement, inclusive));
  }

  public Comparator<? super Cell> comparator() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell first() {
    return this.delegatee.get(this.delegatee.firstKey());
  }

  public Cell last() {
    return this.delegatee.get(this.delegatee.lastKey());
  }

  public boolean add(Cell e) {
    return this.delegatee.put(e, e) == null;
  }
  
  /**
   * Add cell into set, and return the just added cell
   * @param e
   * @return A Pair<Boolean, Cell>
   *  Boolean: Whether the added cell is not present
   *  Cell: The cell stored in the map
   */
  public Pair<Boolean, Cell> addAndFetch(Cell e) {
    if (isEmbededCCSMap) {
      PutAndFetch<Cell> result = ((CompactedConcurrentSkipListMap<Cell, Cell>) delegatee)
          .putAndFetch(e, e);
      return new Pair<Boolean, Cell>(result.old() == null, result.current());
    }
    return new Pair<Boolean, Cell>(this.delegatee.put(e, e) == null, e);
  }

  public boolean addAll(Collection<? extends Cell> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public void clear() {
    this.delegatee.clear();
  }

  public boolean contains(Object o) {
    //noinspection SuspiciousMethodCalls
    return this.delegatee.containsKey(o);
  }

  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public boolean isEmpty() {
    return this.delegatee.isEmpty();
  }

  public boolean remove(Object o) {
    return this.delegatee.remove(o) != null;
  }

  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public Cell get(Cell kv) {
    return this.delegatee.get(kv);
  }

  public int size() {
    return this.delegatee.size();
  }

  public Object[] toArray() {
    throw new UnsupportedOperationException("Not implemented");
  }

  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Not implemented");
  }
  
  void incrScannerReference() {
    if (allocator != null) {
      allocator.incScannerCount();
    }
  }

  void decrScannerReference() {
    if (allocator != null) {
      allocator.decScannerCount();
    }
  }

  void close() {
    if (isEmbededCCSMap) {
      CompactedConcurrentSkipListMap<?, ?> m = (CompactedConcurrentSkipListMap<?, ?>) delegatee;
      if (m != null && LOG.isTraceEnabled()) {
        LOG.trace("Sealed ccsmap, memory summary:  "
            + m.getMemoryUsage().toString());
      }
    }
    if (allocator != null) {
      allocator.close();
    }
  }
  
  MemStoreLAB getAllocator() {
    return this.allocator;
  }

  boolean isEntryUncleanable() {
    return this.isEmbededCCSMap;
  }

  boolean isCCSMap() {
    return this.isEmbededCCSMap;
  }
}
