/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import org.h2.compress.Compressor;
import org.h2.mvstore.type.DataType;
import org.h2.util.New;

/**
 * A page (a node or a leaf).
 * <font color=red><strong>
 * B树的Node节点<br>
 * 因为是B树，所以非叶子节点也存储数据<br>
 * 因此page定义了三个属性keys,values,children
 * <br>--------------------------------<br>
 * page存储在chunk中,每个page的大小是固定的
 * <br>-------------------------------<br>
 * </strong></font>
 * <p>
 * For b-tree nodes, the key at a given index is larger than the largest key of
 * the child at the same index.
 * <p>
 * File format:
 * page length (including length): int
 * check value: short
 * map id: varInt
 * number of keys: varInt
 * type: byte (0: leaf, 1: node; +2: compressed)
 * compressed: bytes saved (varInt)
 * keys
 * leaf: values (one for each key)
 * node: children (1 more than keys)
 */
public class Page {

    /**
     * An empty object array.
     */
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private final MVMap<?, ?> map;
    private long version;
    private long pos;

    /**
     * The total entry count of this page and all children.
     */
    private long totalCount;

    /**
     * The last result of a find operation is cached.
     */
    private int cachedCompare;

    /**
     * The estimated memory used.
     */
    private int memory;

    /**
     * The keys.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] keys;

    /**
     * The values.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] values;

    /**
     * The child page references.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private PageReference[] children;

    /**
     * Whether the page is an in-memory (not stored, or not yet stored) page,
     * and it is removed. This is to keep track of pages that concurrently
     * changed while they are being stored, in which case the live bookkeeping
     * needs to be aware of such cases.
     */
    private volatile boolean removedInMemory;

    Page(MVMap<?, ?> map, long version) {
        this.map = map;
        this.version = version;
    }

    /**
     * Create a new, empty page.
     *
     * @param map the map
     * @param version the version
     * @return the new page
     */
    static Page createEmpty(MVMap<?, ?> map, long version) {
        return create(map, version,
                EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY,
                null,
                0, DataUtils.PAGE_MEMORY);
    }

    /**
     * Create a new page. The arrays are not cloned.
     *
     * @param map the map
     * @param version the version
     * @param keys the keys
     * @param values the values
     * @param children the child page positions
     * @param totalCount the total number of keys
     * @param memory the memory used in bytes
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, long version,
            Object[] keys, Object[] values, PageReference[] children,
            long totalCount, int memory) {
        Page p = new Page(map, version);
        // the position is 0
        p.keys = keys;
        p.values = values;
        p.children = children;
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        MVStore store = map.store;
        if (store != null) {
            /*
             * 向store注册一下，这样store就知道内存中有数据需要刷到磁盘了
             */
            store.registerUnsavedPage(p.memory);
        }
        return p;
    }

    /**
     * Create a copy of a page.
     *
     * @param map the map
     * @param version the version
     * @param source the source page
     * @return the page
     */
    public static Page create(MVMap<?, ?> map, long version, Page source) {
        Page p = new Page(map, version);
        // the position is 0
        p.keys = source.keys;
        p.values = source.values;
        p.children = source.children;
        p.totalCount = source.totalCount;
        p.memory = source.memory;
        MVStore store = map.store;
        if (store != null) {
            store.registerUnsavedPage(p.memory);
        }
        return p;
    }

    /**
     * Read a page.
     *
     * @param fileStore the file store
     * @param pos the position
     * @param map the map
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @return the page
     */
    static Page read(FileStore fileStore, long pos, MVMap<?, ?> map,
            long filePos, long maxPos) {
        /*
         * 从文件中读取一个Page的内容
         */
        ByteBuffer buff;
        /*
         * 从position中，拿到page的最大长度，用来构造ByteBuffer
         */
        int maxLength = DataUtils.getPageMaxLength(pos);
        if (maxLength == DataUtils.PAGE_LARGE) {
            /*
             * 最大长度太长了，保存在磁盘上了，需要从磁盘上读取
             */
            buff = fileStore.readFully(filePos, 128);
            maxLength = buff.getInt();
            // read the first bytes again
        }
        /*
         * 修正maxLength，因为入参也指定了maxPos，我们不应该超过他
         */
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        if (maxLength < 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ",
                    maxLength, filePos, maxPos);
        }
        /*
         * 读磁盘数据
         */
        buff = fileStore.readFully(filePos, maxLength);
        Page p = new Page(map, 0);
        p.pos = pos;
        int chunkId = DataUtils.getPageChunkId(pos);
        int offset = DataUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    /**
     * Get the key at the given index.
     *
     * @param index the index
     * @return the key
     */
    public Object getKey(int index) {
        return keys[index];
    }

    /**
     * Get the child page at the given index.
     *
     * @param index the index
     * @return the child page
     */
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        return ref.page != null ? ref.page : map.readPage(ref.pos);
    }

    /**
     * Get the position of the child.
     *
     * @param index the index
     * @return the position
     */
    public long getChildPagePos(int index) {
        return children[index].pos;
    }

    /**
     * Get the value at the given index.
     *
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        return values[index];
    }

    /**
     * Get the number of keys in this page.
     *
     * @return the number of keys
     */
    public int getKeyCount() {
        return keys.length;
    }

    /**
     * Check whether this is a leaf page.
     *
     * @return true if it is a leaf
     */
    public boolean isLeaf() {
        return children == null;
    }

    /**
     * Get the position of the page
     *
     * @return the position
     */
    public long getPos() {
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        buff.append("id: ").append(System.identityHashCode(this)).append('\n');
        buff.append("version: ").append(Long.toHexString(version)).append("\n");
        buff.append("pos: ").append(Long.toHexString(pos)).append("\n");
        if (pos != 0) {
            int chunkId = DataUtils.getPageChunkId(pos);
            buff.append("chunk: ").append(Long.toHexString(chunkId)).append("\n");
        }
        for (int i = 0; i <= keys.length; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append("[" + Long.toHexString(children[i].pos) + "] ");
            }
            if (i < keys.length) {
                buff.append(keys[i]);
                if (values != null) {
                    buff.append(':');
                    buff.append(values[i]);
                }
            }
        }
        return buff.toString();
    }

    /**
     * Create a copy of this page.
     *
     * @param version the new version
     * @return a page with the given version
     */
    public Page copy(long version) {
        Page newPage = create(map, version,
                keys, values,
                children, totalCount,
                getMemory());
        // mark the old as deleted
        removePage();
        newPage.cachedCompare = cachedCompare;
        return newPage;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     * 
     * 参考Arrays.binarySearch方法：二分查找<br>
     * 二分查找返回的结果，<br>
     *  -->如果>0,则表示找到了数据<br>
     *  -->如果<0,则表示插入点位置<br>
     *  -->如果=-1,表示插入值比当前所有值都小
     * 
     *  
     *
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        int low = 0, high = keys.length - 1;
        // the cached index minus one, so that
        // for the first time (when cachedCompare is 0),
        // the default value is used
        int x = cachedCompare - 1;
        if (x < 0 || x > high) {
            x = high >>> 1;//取中位数，因为low=0，所以简写了(low+high)>>>1
        }
        Object[] k = keys;
        while (low <= high) {
            int compare = map.compare(key, k[x]);
            //key大于中位key，后面应该查(x+1,high)范围
            if (compare > 0) {
                low = x + 1;
            }
            //key小于中位key,后面需要查(low, x-1)范围
            else if (compare < 0) {
                high = x - 1;
            }
            //key等于中位key,命中,返回下标
            else {
                cachedCompare = x + 1;
                return x;
            }
            //没有命中时，二分查找剩下的一半
            x = (low + high) >>> 1;
        }
        cachedCompare = low;
        //如果最终没有命中目标，返回插值点下标
        //插值点的下标应该在(-1,-len-1)之间
        return -(low + 1);

        // regular binary search (without caching)
        // int low = 0, high = keys.length - 1;
        // while (low <= high) {
        //     int x = (low + high) >>> 1;
        //     int compare = map.compare(key, keys[x]);
        //     if (compare > 0) {
        //         low = x + 1;
        //     } else if (compare < 0) {
        //         high = x - 1;
        //     } else {
        //         return x;
        //     }
        // }
        // return -(low + 1);
    }

    /**
     * Split the page. This modifies the current page.
     *
     * @param at the split index
     * @return the page with the entries after the split index
     */
    Page split(int at) {
        return isLeaf() ? splitLeaf(at) : splitNode(at);
    }
    /**
     * 分裂page，分裂后，原page的数据会少一半（左半部分留下了）
     * 返回的是新page
     * 
     * 因为是叶子结点，原来的树结构实际上还是没变的
     * 父结点还是指向原来的子结点，只是多出来了一个新的page对象
     * 
     * @param at
     * @return
     */
    private Page splitLeaf(int at) { //小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;
        totalCount = a;
        Page newPage = create(map, version,
                bKeys, bValues,
                null,
                bKeys.length, 0);
        recalculateMemory();
        //newPage.recalculateMemory(); //create中已经计算过一次了，这里是多于的
        return newPage;
    }

    private Page splitNode(int at) {
        int a = at, b = keys.length - a;

        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;

        PageReference[] aChildren = new PageReference[a + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;

        long t = 0;
        for (PageReference x : aChildren) {
            t += x.count;
        }
        totalCount = t;
        t = 0;
        for (PageReference x : bChildren) {
            t += x.count;
        }
        Page newPage = create(map, version,
                bKeys, null,
                bChildren,
                t, 0);
        recalculateMemory();
        //newPage.recalculateMemory(); //create中已经计算过一次了，这里是多于的
        return newPage;
    }

    /**
     * Get the total number of key-value pairs, including child pages.
     *
     * @return the number of key-value pairs
     */
    public long getTotalCount() {
        if (MVStore.ASSERT) {
            long check = 0;
            if (isLeaf()) {
                check = keys.length;
            } else {
                for (PageReference x : children) {
                    check += x.count;
                }
            }
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL,
                        "Expected: {0} got: {1}", check, totalCount);
            }
        }
        return totalCount;
    }

    /**
     * Get the descendant counts for the given child.
     *
     * @param index the child index
     * @return the descendant count
     */
    long getCounts(int index) {
        return children[index].count;
    }

    /**
     * Replace the child page.
     *
     * @param index the index
     * @param c the new child page
     */
    public void setChild(int index, Page c) {
        if (c == null) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(null, 0, 0);
            children[index] = ref;
            totalCount -= oldCount;
        } else if (c != children[index].page ||
                c.getPos() != children[index].pos) {
            long oldCount = children[index].count;
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(c, c.pos, c.totalCount);
            children[index] = ref;
            totalCount += c.totalCount - oldCount;
        }
    }

    /**
     * Replace the key at an index in this page.
     *
     * @param index the index
     * @param key the new key
     */
    public void setKey(int index, Object key) {
        // this is slightly slower:
        // keys = Arrays.copyOf(keys, keys.length);
        keys = keys.clone();
        Object old = keys[index];
        DataType keyType = map.getKeyType();
        int mem = keyType.getMemory(key);
        if (old != null) {
            mem -= keyType.getMemory(old);
        }
        addMemory(mem);
        keys[index] = key;
    }

    /**
     * Replace the value at an index in this page.
     *
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public Object setValue(int index, Object value) {
        Object old = values[index];
        // this is slightly slower:
        // values = Arrays.copyOf(values, values.length); //只copy引用
        values = values.clone();
        DataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) -
                valueType.getMemory(old));
        values[index] = value;
        return old;
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        if (children != null) {
            //不能直接使用getRawChildPageCount， RTreeMap这样的子类会返回getRawChildPageCount() - 1
            for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    ref.page.removeAllRecursive();
                } else {
                    long c = children[i].pos;
                    int type = DataUtils.getPageType(c);
                    if (type == DataUtils.PAGE_TYPE_LEAF) {
                        int mem = DataUtils.getPageMaxLength(c);
                        map.removePage(c, mem);
                    } else {
                        map.readPage(c).removeAllRecursive();
                    }
                }
            }
        }
        removePage();
    }

    /**
     * Insert a key-value pair into this leaf.
     *
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public void insertLeaf(int index, Object key, Object value) {
        /*
         * byYYY: 将数据插入到叶子节点中，page是一个B-树
         * B-树非叶子节点也存储数据，且是有序的
         * 根据规则，B-树的数据插入，都是先插入到叶子结点上，然后在根据性质，判断是否会分裂
         * 分裂的的数据会向上冒，使得树不断的向上（根部）生长。
         * */
        /*
         * byYYY:这个方法将(k,v)数据插入index处
         *   index是之前通过二分查找获得的插入点位置
         *   因为keys和values都是数组，需要原数组将index位置让出来
         *   所以会通过System.arraycopy来移动数组内容。
         * */
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        values = newValues;
        keys[index] = key;
        values[index] = value;
        totalCount++;
        addMemory(map.getKeyType().getMemory(key) +
                map.getValueType().getMemory(value));
    }

    /**
     * Insert a child page into this node.
     *
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, Page childPage) {

        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(
                childPage, childPage.getPos(), childPage.totalCount);
        children = newChildren;

        totalCount += childPage.totalCount;
        addMemory(map.getKeyType().getMemory(key) +
                DataUtils.PAGE_MEMORY_CHILD);
    }

    /**
     * Remove the key and value (or child) at the given index.
     *
     * @param index the index
     */
    public void remove(int index) {
        int keyLength = keys.length;
        int keyIndex = index >= keyLength ? index - 1 : index;
        Object old = keys[keyIndex];
        addMemory(-map.getKeyType().getMemory(old));
        Object[] newKeys = new Object[keyLength - 1];
        DataUtils.copyExcept(keys, newKeys, keyLength, keyIndex);
        keys = newKeys;

        if (values != null) {
            old = values[index];
            addMemory(-map.getValueType().getMemory(old));
            Object[] newValues = new Object[keyLength - 1];
            DataUtils.copyExcept(values, newValues, keyLength, index);
            values = newValues;
            totalCount--;
        }
        if (children != null) {
            addMemory(-DataUtils.PAGE_MEMORY_CHILD);
            long countOffset = children[index].count;

            int childCount = children.length;
            PageReference[] newChildren = new PageReference[childCount - 1];
            DataUtils.copyExcept(children, newChildren, childCount, index);
            children = newChildren;

            totalCount -= countOffset;
        }
    }

    /**
     * Read the page from the buffer.
     *
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param maxLength the maximum length
     */
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        /*
         * 按照page的磁盘格式，读取page内容
         * page格式
         *      int     page_length
         *      short   check_value,
         *      vint    map_id
         *      vint    key_count
         *      byte    page_type:0--leaf 1--node
         *      [*]     孩子节点的pos( key_count + 1 个Long)
         *      [*]     孩子节点的孩子的个数
         *      [*]     keys
         *      [*]     values
         */
        int start = buff.position();
        int pageLength = buff.getInt();
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}",
                    chunkId, maxLength, pageLength);
        }
        buff.limit(start + pageLength);
        short check = buff.getShort();
        int mapId = DataUtils.readVarInt(buff);
        if (mapId != map.getId()) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected map id {1}, got {2}",
                    chunkId, map.getId(), mapId);
        }
        int checkTest = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}",
                    chunkId, checkTest, check);
        }
        int len = DataUtils.readVarInt(buff);
        keys = new Object[len];
        int type = buff.get();
        boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
        if (node) {
            children = new PageReference[len + 1];
            long[] p = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                p[i] = buff.getLong();
            }
            long total = 0;
            for (int i = 0; i <= len; i++) {
                long s = DataUtils.readVarLong(buff);
                total += s;
                children[i] = new PageReference(null, p[i], s);
            }
            totalCount = total;
        }
        boolean compressed = (type & DataUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & DataUtils.PAGE_COMPRESSED_HIGH) ==
                    DataUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getStore().getCompressorHigh();
            } else {
                compressor = map.getStore().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            buff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, buff.array(),
                    buff.arrayOffset(), l);
        }
        map.getKeyType().read(buff, keys, len, true);
        if (!node) {
            values = new Object[len];
            map.getValueType().read(buff, values, len, false);
            totalCount = len;
        }
        recalculateMemory();
    }

    /**
     * Store the page and update the position.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     * @return the position of the buffer just after the type
     */
    private int write(Chunk chunk, WriteBuffer buff) {
        /*
         * 将当前的page写到buff中
         * page格式：
         *      int     page_length
         *      short   check_value,
         *      vint    map_id
         *      vint    key_count
         *      byte    page_type:0--leaf 1--node
         *      [*]     孩子节点的pos( key_count + 1 个Long)
         *      [*]     孩子节点的孩子的个数
         *      [*]     keys
         *      [*]     values
         */
        int start = buff.position();
        int len = keys.length;
        int type = children != null ? DataUtils.PAGE_TYPE_NODE
                : DataUtils.PAGE_TYPE_LEAF;
        buff.putInt(0).             //1. page length,这里先置0,后面算出长度时再回写
            putShort((byte) 0).     //2. check value
            putVarInt(map.getId()). //3. map id
            putVarInt(len);         //4. key count
        /*
         * 这里记录一下节点type的位置，后面如有有压缩，type还是会变的
         */
        int typePos = buff.position();
        buff.put((byte) type);      //5. page type
        if (type == DataUtils.PAGE_TYPE_NODE) {
            //6. key_count + 1个Long(children的pos)
            writeChildren(buff); //此时pagePos可能为0，在writeUnsavedRecursive中再回填一次
            //7. key_count + 1个VLong(children的key的个数)
            for (int i = 0; i <= len; i++) { //keys.length + 1 才等于 children.length
                buff.putVarLong(children[i].count);
            }
        }
        /*
         * 只压缩keys和values,所以压缩的位置从这里开始
         */
        int compressStart = buff.position();
        //8. 将keys写入buff
        map.getKeyType().write(buff, keys, len, true); //第4个参数目前未使用
        //9. 将values写入buff
        if (type == DataUtils.PAGE_TYPE_LEAF) {
            /*
             * 会根据values的不同类型，调用不同的处理器来写入buff
             */
            map.getValueType().write(buff, values, len, false);
        }
        MVStore store = map.getStore();
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            int compressionLevel = store.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = map.getStore().getCompressorFast();
                    compressType = DataUtils.PAGE_COMPRESSED;
                } else {
                    compressor = map.getStore().getCompressorHigh();
                    compressType = DataUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                //如果是node，只压缩keys，有可能未压缩时的长度就很小，压缩后反而变长，此时就先申请更大的空间先
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).
                        put((byte) (type + compressType));
                    /*
                     * 从压缩开始的地方，重新放置数据
                     */
                    buff.position(compressStart).
                        putVarInt(expLen - compLen).
                        put(comp, 0, compLen);
                }
            }
        }
        int pageLength = buff.position() - start;
        int chunkId = chunk.id;
        int check = DataUtils.getCheckValue(chunkId)
                ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        /*
         * 这里将长度回写回去
         */
        buff.putInt(start, pageLength).
            putShort(start + 4, (short) check);
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(
                    DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        /*
         * 计算page的pos
         */
        pos = DataUtils.getPagePos(chunkId, start, pageLength, type);
        store.cachePage(pos, this, getMemory());
        if (type == DataUtils.PAGE_TYPE_NODE) {
            // cache again - this will make sure nodes stays in the cache
            // for a longer time
            /*
             * 缓存算法：LIRS
             */
            store.cachePage(pos, this, getMemory());
        }
        long max = DataUtils.getPageMaxLength(pos);
        chunk.maxLen += max;
        chunk.maxLenLive += max;
        chunk.pageCount++;
        chunk.pageCountLive++;
        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.removePage(pos, memory);
        }
        return typePos + 1;
    }

    private void writeChildren(WriteBuffer buff) {
        int len = keys.length;
        for (int i = 0; i <= len; i++) {
            buff.putLong(children[i].pos);
        }
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     *
     * @param chunk the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(Chunk chunk, WriteBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(chunk, buff);
        if (!isLeaf()) {
            int len = children.length;
            for (int i = 0; i < len; i++) {
                Page p = children[i].page;
                if (p != null) {
                    p.writeUnsavedRecursive(chunk, buff);
                    children[i] = new PageReference(p, p.getPos(), p.totalCount);
                }
            }
            int old = buff.position();
            buff.position(patch);
            writeChildren(buff); //write(chunk, buff)中的writeChildren可能为0，在这回填一次
            buff.position(old);
        }
    }

    /**
     * Unlink the children recursively after all data is written.
     */
    void writeEnd() {
        if (isLeaf()) {
            return;
        }
        int len = children.length;
        for (int i = 0; i < len; i++) {
            PageReference ref = children[i];
            if (ref.page != null) {
                if (ref.page.getPos() == 0) {
                    throw DataUtils.newIllegalStateException(
                            DataUtils.ERROR_INTERNAL, "Page not written");
                }
                ref.page.writeEnd();
                children[i] = new PageReference(null, ref.pos, ref.count);
            }
        }
    }

    long getVersion() {
        return version;
    }

    public int getRawChildPageCount() {
        return children.length;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof Page) {
            if (pos != 0 && ((Page) other).pos == pos) {
                return true;
            }
            return this == other;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return pos != 0 ? (int) (pos | (pos >>> 32)) : super.hashCode();
    }

    public int getMemory() {
        if (MVStore.ASSERT) {
            int mem = memory;
            recalculateMemory();
            if (mem != memory) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_INTERNAL, "Memory calculation error");
            }
        }
        return memory;
    }

    private void addMemory(int mem) {
        memory += mem;
    }

    private void recalculateMemory() {
        int mem = DataUtils.PAGE_MEMORY;
        DataType keyType = map.getKeyType();
        /*
         * 计算keys占用的内存大小
         */
        for (int i = 0; i < keys.length; i++) {
            mem += keyType.getMemory(keys[i]);
        }
        /*
         * 叶子节点计算，计算values占用的内存大小（叶子节点没有child)
         * 非叶子节点计算children占用的内存大小
         */
        if (this.isLeaf()) {
            DataType valueType = map.getValueType();
            for (int i = 0; i < keys.length; i++) {
                mem += valueType.getMemory(values[i]);
            }
        } else {
            mem += this.getRawChildPageCount() * DataUtils.PAGE_MEMORY_CHILD;
        }
        addMemory(mem - memory);
    }

    void setVersion(long version) {
        this.version = version;
    }

    /**
     * Remove the page.
     */
    public void removePage() {
        long p = pos;
        if (p == 0) {
            removedInMemory = true;
        }
        map.removePage(p, memory);
    }

    /**
     * A pointer to a page, either in-memory or using a page position.
     */
    public static class PageReference {

        /**
         * The position, if known, or 0.
         */
        final long pos;

        /**
         * The page, if in memory, or null.
         */
        final Page page;

        /**
         * The descendant count for this child page.
         */
        final long count;

        public PageReference(Page page, long pos, long count) {
            this.page = page;
            this.pos = pos;
            this.count = count;
        }

    }

    /**
     * Contains information about which other pages are referenced (directly or
     * indirectly) by the given page. This is a subset of the page data, for
     * pages of type node. This information is used for garbage collection (to
     * quickly find out which chunks are still in use).
     */
    public static class PageChildren {

        /**
         * An empty array of type long.
         */
        public static final long[] EMPTY_ARRAY = new long[0];

        /**
         * The position of the page.
         */
        final long pos;

        /**
         * The page positions of (direct or indirect) children. Depending on the
         * use case, this can be the complete list, or only a subset of all
         * children, for example only only one reference to a child in another
         * chunk.
         */
        long[] children;

        /**
         * Whether this object only contains the list of chunks.
         */
        boolean chunkList;

        private PageChildren(long pos, long[] children) {
            this.pos = pos;
            this.children = children;
        }

        PageChildren(Page p) {
            this.pos = p.getPos();
            int count = p.getRawChildPageCount();
            this.children = new long[count];
            for (int i = 0; i < count; i++) {
                children[i] = p.getChildPagePos(i);
            }
        }

        int getMemory() {
            return 64 + 8 * children.length;
        }

        /**
         * Read an inner node page from the buffer, but ignore the keys and
         * values.
         *
         * @param fileStore the file store
         * @param pos the position
         * @param mapId the map id
         * @param filePos the position in the file
         * @param maxPos the maximum position (the end of the chunk)
         * @return the page children object
         */
        static PageChildren read(FileStore fileStore, long pos, int mapId,
                long filePos, long maxPos) {
            ByteBuffer buff;
            int maxLength = DataUtils.getPageMaxLength(pos);
            if (maxLength == DataUtils.PAGE_LARGE) {
                buff = fileStore.readFully(filePos, 128);
                maxLength = buff.getInt();
                // read the first bytes again
            }
            maxLength = (int) Math.min(maxPos - filePos, maxLength);
            int length = maxLength;
            if (length < 0) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "Illegal page length {0} reading at {1}; max pos {2} ",
                        length, filePos, maxPos);
            }
            buff = fileStore.readFully(filePos, length);
            int chunkId = DataUtils.getPageChunkId(pos);
            int offset = DataUtils.getPageOffset(pos);
            int start = buff.position();
            int pageLength = buff.getInt();
            if (pageLength > maxLength) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "File corrupted in chunk {0}, expected page length =< {1}, got {2}",
                        chunkId, maxLength, pageLength);
            }
            buff.limit(start + pageLength);
            short check = buff.getShort();
            int m = DataUtils.readVarInt(buff);
            if (m != mapId) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "File corrupted in chunk {0}, expected map id {1}, got {2}",
                        chunkId, mapId, m);
            }
            int checkTest = DataUtils.getCheckValue(chunkId)
                    ^ DataUtils.getCheckValue(offset)
                    ^ DataUtils.getCheckValue(pageLength);
            if (check != (short) checkTest) {
                throw DataUtils.newIllegalStateException(
                        DataUtils.ERROR_FILE_CORRUPT,
                        "File corrupted in chunk {0}, expected check value {1}, got {2}",
                        chunkId, checkTest, check);
            }
            int len = DataUtils.readVarInt(buff);
            int type = buff.get();
            boolean node = (type & 1) == DataUtils.PAGE_TYPE_NODE;
            if (!node) {
                return null;
            }
            long[] children = new long[len + 1];
            for (int i = 0; i <= len; i++) {
                children[i] = buff.getLong();
            }
            return new PageChildren(pos, children);
        }

        /**
         * Only keep one reference to the same chunk. Only leaf references are
         * removed (references to inner nodes are not removed, as they could
         * indirectly point to other chunks).
         */
        void removeDuplicateChunkReferences() {
            HashSet<Integer> chunks = New.hashSet();
            // we don't need references to leaves in the same chunk
            chunks.add(DataUtils.getPageChunkId(pos));
            for (int i = 0; i < children.length; i++) {
                long p = children[i];
                int chunkId = DataUtils.getPageChunkId(p);
                boolean wasNew = chunks.add(chunkId);
                if (DataUtils.getPageType(p) == DataUtils.PAGE_TYPE_NODE) {
                    continue;
                }
                if (wasNew) {
                    continue;
                }
                removeChild(i--);
            }
        }

        /**
         * Collect the set of chunks referenced directly by this page.
         *
         * @param target the target set
         */
        void collectReferencedChunks(Set<Integer> target) {
            target.add(DataUtils.getPageChunkId(pos));
            for (long p : children) {
                target.add(DataUtils.getPageChunkId(p));
            }
        }

        private void removeChild(int index) {
            if (index == 0 && children.length == 1) {
                children = EMPTY_ARRAY;
                return;
            }
            long[] c2 = new long[children.length - 1];
            DataUtils.copyExcept(children, c2, children.length, index);
            children = c2;
        }

    }

}
