package org.apache.hadoop.hbase.util;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
public class CompactedConcurrentSkipListSet<E> extends AbstractSet<E> implements
    NavigableSet<E>, Cloneable, Serializable {
  
  ConcurrentNavigableMap<E, Object> m; 
  
  CompactedConcurrentSkipListSet(ConcurrentNavigableMap<E,Object> m) {
    this.m = m;
  }

  @Override
  public Comparator<? super E> comparator() {
    return m.comparator();
  }

  @Override
  public E first() {
    return m.firstKey();
  }

  @Override
  public E last() {
    return m.lastKey();
  }

  @Override
  public E ceiling(E e) {
    return m.ceilingKey(e);
  }

  @Override
  public Iterator<E> descendingIterator() {
    return m.descendingKeySet().iterator();
  }

  @Override
  public NavigableSet<E> descendingSet() {
    return m.descendingKeySet();
  }

  @Override
  public E floor(E e) {
    return m.floorKey(e);
  }

  @Override
  public SortedSet<E> headSet(E e) {
    return new CompactedConcurrentSkipListSet<E>(m.headMap(e));
  }

  @Override
  public NavigableSet<E> headSet(E e, boolean inclusive) {
    return new CompactedConcurrentSkipListSet<E>(m.headMap(e, inclusive));
  }

  @Override
  public E higher(E e) {
    return m.higherKey(e);
  }

  @Override
  public E lower(E e) {
    return m.lowerKey(e);
  }

  @Override
  public E pollFirst() {
    Entry<E, Object> e = m.pollFirstEntry();
    return e == null ? null : e.getKey();
  }

  @Override
  public E pollLast() {
    Entry<E, Object> e = m.pollLastEntry();
    return e == null ? null : e.getKey();
  }

  @Override
  public SortedSet<E> subSet(E from, E to) {
    return new CompactedConcurrentSkipListSet<E>(m.subMap(from, to));
  }

  @Override
  public NavigableSet<E> subSet(E from, boolean fromInclusive, E to, boolean toInclusive) {
    return new CompactedConcurrentSkipListSet<E>(m.subMap(from, fromInclusive, to, toInclusive));
  }

  @Override
  public SortedSet<E> tailSet(E e) {
    return new CompactedConcurrentSkipListSet<E>(m.tailMap(e));
  }

  @Override
  public NavigableSet<E> tailSet(E e, boolean inclusive) {
    return new CompactedConcurrentSkipListSet<E>(m.tailMap(e, inclusive));
  }

  @Override
  public Iterator<E> iterator() {
    return m.navigableKeySet().iterator();
  }

  @Override
  public int size() {
    return m.size();
  }

}
