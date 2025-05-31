package org.vanilladb.core.storage.index.ivf;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;

import java.util.ArrayList;
import java.util.List;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKey;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.index.SearchRange;
import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.record.RecordPage;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.CoreProperties;

public class IVFIndex extends Index {

    private static final String SCHEMA_KEY = "key", SCHEMA_RID_BLOCK = "block", SCHEMA_RID_ID = "id";
    public static final int NUM_CLUSTERS;

    static {
        NUM_CLUSTERS = CoreProperties.getLoader().getPropertyAsInteger(
                IVFIndex.class.getName() + ".NUM_CLUSTERS", 100);
    }

    private static String keyFieldName(int index) {
        return SCHEMA_KEY + index;
    }

    private static Schema schema(SearchKeyType keyType) {
        Schema sch = new Schema();
        for (int i = 0; i < keyType.length(); i++)
            sch.addField(keyFieldName(i), keyType.get(i));
        sch.addField(SCHEMA_RID_BLOCK, BIGINT);
        sch.addField(SCHEMA_RID_ID, INTEGER);
        return sch;
    }

    private SearchKey searchKey;
    private RecordFile rf;
    private boolean isBeforeFirsted;
    private List<float[]> centroids = null;

    public IVFIndex(IndexInfo ii, SearchKeyType keyType, Transaction tx) {
        super(ii, keyType, tx);
    }

    @Override
    public void preLoadToMemory() {
        for (int i = 0; i < NUM_CLUSTERS; i++) {
            String tblName = ii.indexName() + "_cluster_" + i + ".tbl";
            long size = fileSize(tblName);
            for (int j = 0; j < size; j++) {
                BlockId blk = new BlockId(tblName, j);
                tx.bufferMgr().pin(blk);
            }
        }
    }

    @Override
    public void beforeFirst(SearchRange searchRange) {
        close();
        if (!searchRange.isSingleValue())
            throw new UnsupportedOperationException();

        this.searchKey = searchRange.asSearchKey();
        int clusterId = getClusterId(searchKey);
        String tblName = ii.indexName() + "_cluster_" + clusterId;
        TableInfo ti = new TableInfo(tblName, schema(keyType));

        this.rf = ti.open(tx, false);

        if (rf.fileSize() == 0)
            RecordFile.formatFileHeader(ti.fileName(), tx);

        rf.beforeFirst();
        isBeforeFirsted = true;
    }

    @Override
    public boolean next() {
        if (!isBeforeFirsted)
            throw new IllegalStateException("You must call beforeFirst() before iterating index '" + ii.indexName() + "'");

        while (rf.next()) {
            if (getKey().equals(searchKey))
                return true;
        }
        return false;
    }

    @Override
    public RecordId getDataRecordId() {
        long blkNum = (Long) rf.getVal(SCHEMA_RID_BLOCK).asJavaVal();
        int id = (Integer) rf.getVal(SCHEMA_RID_ID).asJavaVal();
        return new RecordId(new BlockId(dataFileName, blkNum), id);
    }

    @Override
    public void insert(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        beforeFirst(new SearchRange(key));

        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        rf.insert();
        for (int i = 0; i < keyType.length(); i++)
            rf.setVal(keyFieldName(i), key.get(i));
        rf.setVal(SCHEMA_RID_BLOCK, new BigIntConstant(dataRecordId.block().number()));
        rf.setVal(SCHEMA_RID_ID, new IntegerConstant(dataRecordId.id()));

        if (doLogicalLogging)
            tx.recoveryMgr().logIndexInsertionEnd(ii.indexName(), key, dataRecordId.block().number(), dataRecordId.id());
    }

    @Override
    public void delete(SearchKey key, RecordId dataRecordId, boolean doLogicalLogging) {
        beforeFirst(new SearchRange(key));

        if (doLogicalLogging)
            tx.recoveryMgr().logLogicalStart();

        while (next()) {
            if (getDataRecordId().equals(dataRecordId)) {
                rf.delete();
                break;
            }
        }

        if (doLogicalLogging)
            tx.recoveryMgr().logIndexDeletionEnd(ii.indexName(), key, dataRecordId.block().number(), dataRecordId.id());
    }

    public static long searchCost(SearchKeyType keyType, long totRecs, long matchRecs) {
        int rpb = Buffer.BUFFER_SIZE / RecordPage.slotSize(schema(keyType));
        return (totRecs / rpb) / NUM_CLUSTERS;
    }

    @Override
    public void close() {
        if (rf != null)
            rf.close();
    }

    private long fileSize(String fileName) {
        tx.concurrencyMgr().readFile(fileName);
        return VanillaDb.fileMgr().size(fileName);
    }

    private SearchKey getKey() {
        Constant[] vals = new Constant[keyType.length()];
        for (int i = 0; i < keyType.length(); i++)
            vals[i] = rf.getVal(keyFieldName(i));
        return new SearchKey(vals);
    }

    private void loadCentroids() {
        if (centroids != null)
            return;

        centroids = new ArrayList<>();
        String centroidTblName = ii.indexName() + "_centroids";
        TableInfo ti = new TableInfo(centroidTblName, schema(keyType));

        RecordFile rf = ti.open(tx, false);
        rf.beforeFirst();
        while (rf.next()) {
            float[] vec = new float[keyType.length()];
            for (int i = 0; i < vec.length; i++)
                vec[i] = (Float) rf.getVal(keyFieldName(i)).asJavaVal();
            centroids.add(vec);
        }
        rf.close();
    }

    private int getClusterId(SearchKey key) {
        loadCentroids();

        float[] queryVec = new float[keyType.length()];
        for (int i = 0; i < keyType.length(); i++)
            queryVec[i] = (Float) key.get(i).asJavaVal();

        int nearestIdx = -1;
        double minDist = Double.MAX_VALUE;

        for (int i = 0; i < centroids.size(); i++) {
            float[] centroid = centroids.get(i);
            double dist = 0;
            for (int j = 0; j < centroid.length; j++) {
                double diff = queryVec[j] - centroid[j];
                dist += diff * diff;
            }
            if (dist < minDist) {
                minDist = dist;
                nearestIdx = i;
            }
        }
        return nearestIdx;
    }
}
