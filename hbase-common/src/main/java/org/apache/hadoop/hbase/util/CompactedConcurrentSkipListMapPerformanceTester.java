package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.io.Files;

@InterfaceAudience.Private
public class CompactedConcurrentSkipListMapPerformanceTester {
  static class IntegerTypeHelper implements CompactedTypeHelper<Integer, String> {
    @Override
    public int getCompactedSize(Integer key, String value) {
      return 4 + value.length();
    }
    
    private void copyIntToArray(int val, byte[] data, int offset) {
      for (int pos = offset; pos < offset + 4; ++pos) {
        data[pos] = (byte) (val & BYTE);
        val = val >> 8;
      }
    }
    
    static int BYTE = 0xFF;
    
    private final int getIntFromArray(byte[] data, int offset) {
      int ret = 0;
      ret = ((data[offset + 3] & BYTE) << 24) | ((data[offset + 2] & BYTE)) << 16
          | ((data[offset + 1] & BYTE) << 8) | (data[offset] & BYTE); 
      return ret;
    }

    @Override
    public void compact(Integer key, String value, byte[] data, int offset,
        int len) {
      copyIntToArray(key.intValue(), data, offset);
      //byte[] src = value.getBytes();
      //System.arraycopy(src, 0, data, offset + 4, src.length);
    }
    
    static private String value = new String("emptyvalue");

    @Override
    public CompactedTypeHelper.KVPair<Integer, String> decomposte(
        byte[] data, int offset, int len) {
      int intkey = getIntFromArray(data, offset);
      // we don't care which value it is
      return new KVPair<Integer, String>(Integer.valueOf(intkey), value);
    }
    
    private int compare(int key1, int key2) {
      if (key1 < key2) {
        return -1;
      } else if (key2 < key1) {
        return 1;
      } else {
        return 0;
      }
    }

    @Override
    public int compare(byte[] data1, int offset1, int len1, byte[] data2,
        int offset2, int len2) {
      int key1 = getIntFromArray(data1, offset1);
      int key2 = getIntFromArray(data2, offset2);
      return compare(key1, key2);
    }

    @Override
    public int compare(Integer key, byte[] data, int offset, int len) {
      return compare(key.intValue(), getIntFromArray(data, offset));
    }

    @Override
    public int compare(Integer key1, Integer key2) {
      return compare(key1.intValue(), key2.intValue());
    }
    
  }
  
  static abstract class AbstractMap {
    protected Config conf;
    AbstractMap(Config conf) {
      this.conf = conf;
    }
    abstract boolean put(Random random);
    abstract boolean put(Random random, AbstractMap anothor);
    abstract boolean get(Random random);
    abstract public long size();
  }
  
  static class IntegerMap extends AbstractMap {
    final Map<Integer, String> embededMap;
    final int keyRange;
    Map<Integer, String> createMapForTest() {
      if (conf.isCompcatedMap) {
        return new CompactedConcurrentSkipListMap<Integer, String>(
            new IntegerTypeHelper(), CompactedConcurrentSkipListMap.PageSetting.create()
                .setDataPageSize(conf.dataPageSize));
      } else {
        return new ConcurrentSkipListMap<Integer, String>();
      }
    }
    
    static ThreadLocal<String> staticValue = new ThreadLocal<String>();
    static String createValue(int len) {
      String ret = staticValue.get();
      if (ret != null && ret.length() == len)  {
        return new String(ret);
      } else {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; ++i) {
          sb.append('0');
        }
        staticValue.set(sb.toString());
        return new String(staticValue.get());
      }
    }

    IntegerMap(Config conf) {
      super(conf);
      this.embededMap = createMapForTest();
      this.keyRange = conf.getKeyDistirbuteRange();
    }

    @Override
    boolean put(Random random) {
      return  (embededMap.put(random.nextInt(), createValue(conf.getValueLength())) == null);
    }

    @Override
    boolean get(Random random) {
      int key = random.nextInt(this.keyRange);
      if (random.nextBoolean()) {
        key = -key;
      }
      String ret = embededMap.get(key);
      return (ret != null);
    }

    @Override
    boolean put(Random random, AbstractMap anothor) {
      IntegerMap a = (IntegerMap) anothor;
      int key = random.nextInt();
      String value = createValue(conf.getValueLength());
      this.embededMap.put(key, value);
      return (a.embededMap.put(key, value) == null);
    }

    @Override
    public long size() {
      return embededMap.size();
    }
  }
  
  static class KeyValueMap extends AbstractMap {
    final ConcurrentNavigableMap<KeyValue, KeyValue> embededMap;
    final int keyRange;
    KeyValueMap(Config conf) {
      super(conf);
      this.embededMap = createMapForTest(conf);
      this.keyRange = conf.getKeyDistirbuteRange();
    }
    
    
    static ConcurrentNavigableMap<KeyValue, KeyValue> createMapForTest(Config config) {
      if (config.isCompcatedMap) {
        CompactedKeyValueTypeHelper typeHelper = new CompactedKeyValueTypeHelper(
            KeyValue.COMPARATOR);
        return new CompactedConcurrentSkipListMap<KeyValue, KeyValue>(
            typeHelper, CompactedConcurrentSkipListMap.PageSetting.create()
                .setDataPageSize(config.dataPageSize));
      } else {
        return new ConcurrentSkipListMap<KeyValue, KeyValue>(KeyValue.COMPARATOR);
      }
    }
    
    static ThreadLocal<KeyValueGenerator> kvgs = new ThreadLocal<KeyValueGenerator>();
    
    private KeyValueGenerator getKeyValueGenerator() {
      KeyValueGenerator ret = kvgs.get();
      if (ret == null) {
        ret = new KeyValueGenerator(conf);
        kvgs.set(ret);
      }
      return ret;
    }

    @Override
    boolean put(Random random) {
      KeyValueGenerator kvg = getKeyValueGenerator();
      KeyValue kv = kvg.next(random);
      return (embededMap.put(kv, kv) == null);
    }


    @Override
    boolean get(Random random) {
      KeyValueGenerator kvg = getKeyValueGenerator();
      KeyValue kv = kvg.nextForRead(random);
      return (embededMap.get(kv) != null);
    }


    @Override
    boolean put(Random random, AbstractMap anothor) {
      KeyValueMap a = (KeyValueMap) anothor;
      KeyValueGenerator kvg = getKeyValueGenerator();
      KeyValue kv = kvg.next(random);
      this.embededMap.put(kv, kv);
      return (a.embededMap.put(kv, kv) == null);
    }


    @Override
    public long size() {
      return embededMap.size();
    }
  }
  
  
  static class Config {
    int dataPageSize = 256 * 1024;
    int metaPageSize = 128 * 1024;
    int rowKeylength = 10;
    int qualifier = 3;
    int familyLength = 3;
    int vlen = 30; // total length of key-value
    int threads = 8;
    
    boolean isCompcatedMap = true;
    int mapSize = 200 * 1024 * 1024;
    int presentMap = 32;
    int testingTime = 0;  // seconds
    boolean saveOutput = false;

    final static String READ = "read";
    final static String WRITE = "write";
    final static String MEMORY = "memory";
    
    String type = "write";
    
    final static String SCENE_KV = "kv";
    final static String SCENE_INT = "int";
    
    String scene = "kv";
    
    
    
    int valueLength = 0;
    
    
    
    public Config() {
    }
    
    public Config(Config conf) {
      this.dataPageSize = conf.dataPageSize;
      this.metaPageSize = conf.metaPageSize;
      this.vlen = conf.vlen;
      this.threads = conf.threads;
      this.isCompcatedMap = conf.isCompcatedMap;
      this.mapSize = conf.mapSize;
      this.presentMap = conf.presentMap;
      this.testingTime = conf.testingTime;
      this.type = conf.type;
      this.rowKeylength = conf.rowKeylength;
      this.familyLength = conf.familyLength;
      this.scene = conf.scene;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("test=").append(type).append('\n');
      sb.append("vlen=").append(vlen).append('\n');
      sb.append("threads=").append(threads).append('\n');
      sb.append("dataPageSize=").append(dataPageSize).append('\n');
      sb.append("mataPageSize=").append(metaPageSize).append('\n');
      sb.append("qualifierLen=").append(qualifier).append('\n');
      sb.append("familyLen=").append(familyLength).append('\n');
      sb.append("mapSize=").append(mapSize).append('\n');
      sb.append("scene=").append("scene").append('\n');
      sb.append("saveoutput=").append(saveOutput).append('\n');
      if (this.type.equals(WRITE)) {
        sb.append("isCompcatedMap=").append(isCompcatedMap).append('\n'); 
      }
      sb.append("testingtime=").append(testingTime).append('\n');
      return sb.toString();
    }
    
    public int getEntryCount() {
      return mapSize / vlen;
    }
    
    public String getMapType() {
      return (isCompcatedMap ? "COMPACTED" : "STAND");
    }
    
    public int getKeyDistirbuteRange() {
      int factor = scene.equals(SCENE_KV) ? 5 : 2;
      int range = (mapSize / vlen) * factor;
      range = range < 1024 ? 1024 : range;
      return range;
    }
    
    public int getValueLength() {
      if (valueLength == 0) {
        int v = 0;
        if (this.scene.equals(SCENE_KV)) {
          v = vlen - (Long.SIZE >> 3) - rowKeylength - qualifier - familyLength;
        } else {
          v = vlen - (Integer.SIZE >> 3);
        }
        valueLength =  v < 0 ? 1 : v;
      }
      
      return valueLength;
    }
    
    public void loadConfigFromProperty() {
      isCompcatedMap = Boolean.valueOf(System.getProperty("test.compact", "true"));
      threads = Integer.valueOf(System.getProperty("test.threads", Integer.toString(threads)));
      presentMap = Integer.valueOf(System.getProperty("test.maps", Integer.toString(presentMap)));
      vlen = Integer.valueOf(System.getProperty("test.vlen", Integer.toString(vlen)));
      dataPageSize = metaPageSize = Integer.valueOf(System.getProperty("test.pagesize", Integer.toString(dataPageSize)));
      mapSize = Integer.valueOf(System.getProperty("test.mapsize", Integer.toString(this.mapSize)));
      type = System.getProperty("test.type", "write");
      rowKeylength = Integer.valueOf(System.getProperty("test.rowkey", Integer.toString(rowKeylength)));
      qualifier = Integer.valueOf(System.getProperty("test.qualifier", Integer.toString(qualifier)));
      familyLength = Integer.valueOf(System.getProperty("test.familiy", Integer.toString(familyLength)));
      scene = System.getProperty("test.scene", SCENE_KV);
      saveOutput =  Boolean.valueOf(System.getProperty("test.sav", Boolean.toString(saveOutput)));
      if (WRITE.equals(type)) {
        testingTime = Integer.valueOf(System.getProperty("test.time", Integer.toString(300)));
      } else if (READ.equals(type)){
        testingTime = Integer.valueOf(System.getProperty("test.time", Integer.toString(120)));
      }
    }
    
    public String getCaseName() {
      StringBuilder sb = new StringBuilder();
      sb.append(scene).append('_').append(type).append("_vlen").append(vlen);
      if (!READ.equals(this.type)) {
        sb.append("_").append(getMapType());
      }
      return sb.toString();
    }
  }
  
  static AbstractMap createSceneMap(Config config) {
    if (Config.SCENE_KV.equals(config.scene)) {
      return new KeyValueMap(config);
    } else if (Config.SCENE_INT.equals(config.scene)){
      return new IntegerMap(config);
    } else {
      throw new IllegalArgumentException("Invalid scene: " + config.scene);
    }
  }
  
  public static int DEFAULT_DATA_PAGE_SIZE = 256 * 1024; // 256k
  public static int DEFAULT_DATA_PAGE = 16 * 1024; // 16k
  public static int DEFAULT_META_PAGE_SIZE = 128 * 1024; // 128k
  public static int DEFAULT_META_PAGE = 8 * 1024; // 8k  
  public static int DEFAULT_OUT_PAGE_PAGE_SIZE = 1024;
  public static int DEFAULT_OUT_PAGE = 1024;
  public static int DEFAULT_BIG_KV_THRESHOLD = 16 * 1024; // 16k
  static ThreadLocal<Random> random = new ThreadLocal<Random>();
  static class MapContainer  {
    AtomicReferenceArray<AbstractMap> maps;
    List<AtomicInteger> mapSize = new ArrayList<AtomicInteger>();
    Config conf;
    
    public MapContainer(Config config) {
      this.conf = config;
      maps = new AtomicReferenceArray<AbstractMap>(conf.presentMap);
      for (int i = 0; i < conf.presentMap; ++i) {
        maps.set(i, createSceneMap(config));
        mapSize.add(new AtomicInteger(0));
      }
    }

    int putOneRecord() {
      // init thread local random object
      Random r = null;
      r = getThreadLocalRandom();
      
      for (;;) {
        int mapIdx = r.nextInt(conf.presentMap);
        int curSize = mapSize.get(mapIdx).get();
        if (curSize < 0 || curSize > conf.mapSize) {
          // map needs updating, leave
          continue;
        }
        AbstractMap curMap = maps.get(mapIdx);
        if (curMap == null) {
          // map is swapping, leave
          continue;
        }
        curMap.put(r);
        curSize = mapSize.get(mapIdx).addAndGet(4 + conf.vlen);
        if (curSize > conf.mapSize) {
          // try new map; acquire lock by null pointer
          if (maps.compareAndSet(mapIdx, curMap, null)) {
            System.out.println("Swaping map at slot " + mapIdx);
            maps.set(mapIdx, createSceneMap(conf));
            mapSize.get(mapIdx).set(0);
          }
        }
        return 4 + conf.vlen;
      }
      
    }
  }
  
  static class KeyValueGenerator {
    
    final Config conf;
    final int range;
    
    byte[] rowKeyBuffer;
    byte[] qualifierBuffer;
    byte[] familyBuffer;
    byte[] valueBuffer;
    public KeyValueGenerator(Config config) {
      this.conf = config;
      range = config.getKeyDistirbuteRange();
      rowKeyBuffer = new byte[config.rowKeylength];
      qualifierBuffer = new byte[config.qualifier];
      familyBuffer = new byte[config.familyLength];
      valueBuffer = new byte[config.getValueLength()];
      
      // set value
      for (int i = 0; i < valueBuffer.length; ++i) {
        valueBuffer[i] = '0';
      }
      // set family
      for (int i = 0; i < familyBuffer.length; ++i) {
        familyBuffer[i] = 'a';
      }
    }
    
    void fillBufferFromInt(int v, byte[] buf) {
      for (int i = 0; i < buf.length; ++i) {
        buf[i] = (byte)('A' + (v & 0x1F));
        v = v >> 5;
      }
    }
    
    public KeyValue next(Random random) {
      int k = random.nextInt(range);
      fillBufferFromInt(k, rowKeyBuffer);
      fillBufferFromInt(k, qualifierBuffer);
      KeyValue ret = new KeyValue(rowKeyBuffer, familyBuffer, qualifierBuffer,
          k, Type.Put, valueBuffer);
      return ret;
    }
    
    public KeyValue nextForRead(Random random) {
      int k = random.nextInt(range);
      fillBufferFromInt(k, rowKeyBuffer);
      fillBufferFromInt(k, qualifierBuffer);
      KeyValue ret = new KeyValue(rowKeyBuffer, familyBuffer, qualifierBuffer, k,
          Type.Put);
      return ret;
    }
  }
  
  static ThreadLocal<String> staticValue = new ThreadLocal<String>();


  
  static void doReadTest(Config conf) {
    Config conf1 = new Config(conf); conf1.isCompcatedMap = true;
    Config conf2 = new Config(conf); conf2.isCompcatedMap = false;
    AbstractMap mCompcated = createSceneMap(conf1);
    AbstractMap mStand = createSceneMap(conf2);
    
    System.out.println("Building map");
    Random r = new Random();
    int elementCount = 0;
    
    for (int i = 0; i < conf.getEntryCount(); ++i) {
      if (mCompcated.put(r, mStand)) {
        elementCount++;
      }
      if (i % 100000 == 0) {
        System.out.println("Building map i=" + i);
      }
    }
    System.out.println("There are " + elementCount + " element in map");
    double spd1 = testMapRead(conf1, mCompcated);
    double spd2 = testMapRead(conf2, mStand);
    
    System.out.println("\nCompcatd map read speed: " + spd1);
    System.out.println("\nStand map read speed: " + spd2);
  }
  
  static double testMapRead(Config conf, AbstractMap map) {
    AtomicLong counter = new AtomicLong(0);
    System.out.println("\n\nStart doing read test for " + conf.getMapType() + " map");
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < conf.threads; ++i) {
      Thread t = new Thread(runningReadThread(map, conf, counter), "Get-Thread-" + i);
      threads.add(t);
    }
    
    for (Thread t : threads) {
      t.start();
    }
    
    long start = System.currentTimeMillis();
    long now = start + 1;
    try {
      do {
        long last = counter.get();
        long startLine = System.currentTimeMillis();
        Thread.sleep(1000);
        now = System.currentTimeMillis();
        long time = now - startLine;
        long qps = counter.get() - last;
        System.out.println("Current qps: " + qps * 1000 / (double)time);
      } while (now - start < conf.testingTime * 1000);
    } catch (InterruptedException ie) {
    }
    
    for (Thread t : threads) {
      t.interrupt();
      t.stop();
    }

    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
        //ignore
      }
    }
    
    double spd = counter.get() * 1000 / (double)(now - start);
    System.out.println("Average qps for "
        + (conf.isCompcatedMap ? "compcated" : "stand") + " map: "
        + spd);
    return spd;
  }
  
  static Runnable runningReadThread(final AbstractMap m, final Config conf, final AtomicLong counter) {
    return new Runnable() {
      
      @Override
      public void run() {
        Random r = getThreadLocalRandom();

        
        for (;;) {
          m.get(r);
          counter.incrementAndGet();
        }
      }
    };
  }
  
  static Runnable runningWriteThread(final MapContainer mc,
      final AtomicLong counter) {
    return new Runnable() {
      @Override
      public void run() {
        while (true) {
          int res = mc.putOneRecord();
          counter.addAndGet(res);
        }
      }
    };
  }
  
  static void redirectOutput(Config conf) throws FileNotFoundException {
    String outputFileName = conf.getCaseName() + ".out";
    System.out.println("Redirecing output to file " + outputFileName);
    PrintStream ps = new PrintStream(outputFileName);
    System.setOut(ps);
  }
  
  static void copyGCLog(Config conf) throws IOException {
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = runtimeMxBean.getInputArguments();
    String src = null;
    for (String s : arguments) {
      if (s.startsWith("-Xloggc:")) {
        src = s.replaceAll("-Xloggc:", "");
        break;
      }
    }
    if (src != null) {
      String dst = conf.getCaseName() + "_gc.log";
      System.out.println("Copy gc log from " + src + " to " + dst);
      Files.copy(new File(src), new File(dst));
    } else {
      System.out.println("GC log not found in vm arguments!");
    }
  }

  public static void main(String[] args) throws IOException {
    System.out.println("Starting performace test for CompactdConcurrentSkipListMap");
    Config conf = new Config();
    conf.loadConfigFromProperty();
    if (conf.saveOutput) {
      redirectOutput(conf);
    }
    

    System.out.println("Config:");
    System.out.println(conf.toString());
    
    if (Config.WRITE.equals(conf.type)) {
      doWriteTest(conf);
    } else if (Config.READ.equals(conf.type)) {
      doReadTest(conf);
    } else if (Config.MEMORY.equals(conf.type)) {
      doMemoryTest(conf);
    }
    
    copyGCLog(conf);
  }
  
  static AbstractMap[] maps;
  
  @SuppressWarnings("DM_GC")
  private static void doMemoryTest(Config conf) {
    
    
    Random r = new Random() {
      int last = 0;

      @Override
      public int nextInt() {
        last += 1;
        return last;
      }
      
      @Override
      public int nextInt(int range) {
        return nextInt() % range;
      }
    };
    
    maps = new AbstractMap[conf.presentMap];
    System.out.println("Going to put " + conf.getEntryCount()
        + " elements into " + maps.length +" " + conf.getMapType() + " map");
    
    int entryCount = conf.getEntryCount();
    for (int i = 0; i < maps.length; ++i) {
      AbstractMap m = createSceneMap(conf);
      maps[i] = m;
      for (int j = 0; j < entryCount; ++j) {
        m.put(r);
      }
    }
    
    long elementCount = 0;
    for (int i = 0; i < maps.length; ++i) {
      elementCount += maps[i].size();
    }
    System.out.println("There are " + elementCount + " entries in " + maps.length + " map");
    System.out.println("gc starting..");
    System.gc();
    System.out.println("gc finished..");
    System.out.println("Free memory: " + Runtime.getRuntime().freeMemory());
    System.out.println("Total memory: " + Runtime.getRuntime().totalMemory());
    System.out.println("Used memory: " +  (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 + " KB");
  }

  private static void doWriteTest(Config conf) {
    MapContainer mc = new MapContainer(conf);
    AtomicLong counter = new AtomicLong();
    List<Thread> threads = new ArrayList<Thread>();
    for (int i = 0; i < conf.threads; ++i) {
      threads.add(new Thread(runningWriteThread(mc, counter), "Put-Thread-" + i));
    }
    for (Thread t : threads) {
      t.start();
    }
    
    long start = System.currentTimeMillis();
    long now = start + 1;
    try {
      do {
        long startLine = System.currentTimeMillis();
        long last = counter.get();
        Thread.sleep(1000);
        now = System.currentTimeMillis();
        long time = now - startLine;
        long data = counter.get() - last;
        System.out.println("Current through put: " + (data * 1000 / 1024) / (double) time);
      } while ((now - start) < conf.testingTime * 1000);
    } catch (InterruptedException e) { 
    }
    
    
    for (Thread t : threads) {
      t.interrupt();
      t.stop();
    }
    
    try {
      for (Thread t : threads) {
        t.join();
      }
    } catch (InterruptedException e) {
    }
    
    System.out.println("Average write through put: "
        + (counter.get() * 1000 / 1024) / (double) (now - start) + " kb/s");
  }

  private static Random getThreadLocalRandom() {
    Random r;
    if ((r = random.get()) == null) {
      random.set(new Random());
      r = random.get();
    }
    return r;
  }
  
  @InterfaceAudience.Private
  static public class CompactedKeyValueTypeHelper
      implements CompactedTypeHelper<KeyValue, KeyValue> {

    private KeyValue.KVComparator comparator;

    public CompactedKeyValueTypeHelper(KeyValue.KVComparator kvCompare) {
      this.comparator = kvCompare;
    }

    public final static int lengthWithoutMemstore(int len) {
      return len - Bytes.SIZEOF_LONG;
    }

    @Override
    public int getCompactedSize(KeyValue key, KeyValue value) {
      return key.getLength() + Bytes.SIZEOF_LONG; // mvcc's size
    }

    @Override
    public void compact(KeyValue key, KeyValue value, byte[] data, int offset,
        int len) {
      assert len == key.getLength() + Bytes.SIZEOF_LONG;
      System.arraycopy(key.getBuffer(), key.getOffset(), data, offset,
          key.getLength());
      long mts = key.getMvccVersion();
      int mo = offset + lengthWithoutMemstore(len);
      data[mo] = (byte) (mts >> 56);
      data[mo + 1] = (byte) (mts >> 48);
      data[mo + 2] = (byte) (mts >> 40);
      data[mo + 3] = (byte) (mts >> 32);
      data[mo + 4] = (byte) (mts >> 24);
      data[mo + 5] = (byte) (mts >> 16);
      data[mo + 6] = (byte) (mts >> 8);
      data[mo + 7] = (byte) (mts);
    }

    @Override
    public KVPair<KeyValue, KeyValue> decomposte(byte[] data, int offset,
        int len) {
      KeyValue kv = new KeyValue(data, offset, lengthWithoutMemstore(len));
      kv.setSequenceId(getMvccFromRaw(data, offset, len));
      return new KVPair<KeyValue, KeyValue>(kv, kv);
    }

    public final static long getMvccFromRaw(byte[] data, int offset, int len) {
      int mo = offset + lengthWithoutMemstore(len);
      return (data[mo] & 0xFFL) << 56 | (data[mo + 1] & 0xFFL) << 48
          | (data[mo + 2] & 0xFFL) << 40 | (data[mo + 3] & 0xFFL) << 32
          | (data[mo + 4] & 0xFFL) << 24 | (data[mo + 5] & 0xFFL) << 16
          | (data[mo + 6] & 0xFFL) << 8 | (data[mo + 7] & 0xFFL);
    }

    private final static int keyLength(byte[] data, int offset) {
      return Bytes.toInt(data, offset);
    }

    private final static int rawCompare(KeyValue.KVComparator comparator,
        byte[] ldata, int loffset, int llen, byte[] rdata, int roffset,
        int rlen) {
      int ret = comparator.compare(ldata, loffset + KeyValue.ROW_OFFSET,
          keyLength(ldata, loffset), rdata, roffset + KeyValue.ROW_OFFSET,
          keyLength(rdata, roffset));
      if (ret != 0)
        return ret;
      // Negate this comparison so later edits show up first
      return -Long.compare(getMvccFromRaw(ldata, loffset, llen),
          getMvccFromRaw(rdata, roffset, rlen));
    }

    @Override
    public int compare(byte[] ldata, int loffset, int llen, byte[] rdata,
        int roffset, int rlen) {
      return rawCompare(comparator, ldata, loffset, llen, rdata, roffset, rlen);
    }

    @Override
    public int compare(KeyValue key, byte[] rdata, int roffset, int rlen) {
      int ret = comparator.compare(key.getBuffer(),
          key.getOffset() + KeyValue.ROW_OFFSET, key.getKeyLength(), rdata,
          roffset + KeyValue.ROW_OFFSET, keyLength(rdata, roffset));
      if (ret != 0)
        return ret;
      // Negate this comparison so later edits show up first
      return -Long.compare(key.getMvccVersion(),
          getMvccFromRaw(rdata, roffset, rlen));
    }

    @Override
    public int compare(KeyValue lkey, KeyValue rkey) {
      return comparator.compare(lkey, rkey);
    }
  }
}
