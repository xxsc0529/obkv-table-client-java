/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Executors;

public class ObTableClientPartitionRangeTest {
    private ObTableClient obTableClient;

    @Before
    public void setUp() throws Exception {
        /*
         *
         * create table testRange (K varbinary(1024), Q varbinary(256), T bigint, V varbinary(102400), primary key(K, Q, T)) partition by range columns (K) (PARTITION p0 VALUES LESS THAN ('a'), PARTITION p1 VALUES LESS THAN ('w'), PARTITION p2 VALUES LESS THAN MAXVALUE);
         *
         * */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.setTableEntryAcquireSocketTimeout(10000);
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
        obTableClient.init();

        this.obTableClient = obTableClient;
    }

    @After
    public void close() throws Exception {
        if (null != this.obTableClient) {
            ((ObTableClient) this.obTableClient).close();
        }
    }

    @Test
    public void testInsert() throws Exception {

        long timestamp = System.currentTimeMillis();
        try {
            long affectRow = obTableClient.insert("testRange", new Object[] {
                            "partitionKey".getBytes(), "partition".getBytes(), timestamp },
                    new String[] { "V" }, new Object[] { "aa".getBytes() });
            Assert.assertEquals(1, affectRow);

            affectRow = obTableClient.insertOrUpdate("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timestamp },
                    new String[] { "V" }, new Object[] { "bb".getBytes() });
            Assert.assertEquals(1, affectRow);

            Map<String, Object> result = obTableClient.get("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timestamp },
                    new String[] { "K", "Q", "T", "V" });
            Assert.assertEquals("partitionKey", new String((byte[]) result.get("K"), "UTF-8"));
            Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
            Assert.assertEquals(timestamp, result.get("T"));
            Assert.assertEquals("bb", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange", new Object[] {
                    "partitionKey".getBytes(), "partition".getBytes(), timestamp });
            obTableClient.delete("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timestamp });
        }

    }

    @Test
    public void testGet() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value".getBytes() });
            Map<String, Object> result = obTableClient.get("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "K", "Q", "T", "V" });
            Assert.assertEquals("partitionKey", new String((byte[]) result.get("K"), "UTF-8"));
            Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
            Assert.assertEquals(timeStamp, result.get("T"));
            Assert.assertEquals("value", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {

        } finally {
            obTableClient.delete("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testUpdate() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert("testRange",
                    new Object[]{"partitionKey".getBytes(), "partition".getBytes(), timeStamp},
                    new String[]{"V"}, new Object[]{"value".getBytes()});
            long affectedRow = obTableClient.update("testRange",
                    new Object[]{"partitionKey".getBytes(), "partition".getBytes(), timeStamp},
                    new String[]{"V"}, new Object[]{"value1".getBytes()});
            Assert.assertEquals(1, affectedRow);
            Map<String, Object> result = obTableClient.get("testRange",
                    new Object[]{"partitionKey".getBytes(), "partition".getBytes(), timeStamp},
                    new String[]{"K", "Q", "T", "V"});
            Assert.assertEquals(timeStamp, result.get("T"));
            Assert.assertEquals("value1", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
           e.printStackTrace();
           Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange",
                    new Object[]{"partitionKey".getBytes(), "partition".getBytes(), timeStamp});
        }

    }

    @Test
    public void testReplace() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value".getBytes() });
            long affectedRow = obTableClient.replace("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            Assert.assertEquals(2, affectedRow);
            Map<String, Object> result = obTableClient.get("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "K", "Q", "T", "V" });
            Assert.assertEquals(timeStamp, result.get("T"));
            Assert.assertEquals("value1", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange",
                    new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testDelete() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testRange",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.delete("testRange",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp });
        Assert.assertEquals(1, affectedRow);
        Map<String, Object> result = obTableClient.get("testRange",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "K", "Q", "T", "V" });
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testQuery() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            // p0
            obTableClient.insert("testRange",
                    new Object[] { "0", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            // p1
            obTableClient.insert("testRange",
                    new Object[] { "ah", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testRange",
                    new Object[] { "uh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            // p2
            obTableClient.insert("testRange",
                    new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });

            // single partition query
            TableQuery tableQuery = obTableClient.query("testRange");
            tableQuery.addScanRange(new Object[] { "ah", "partition".getBytes(), timeStamp },
                                    new Object[] { "az", "partition".getBytes(), timeStamp });
            tableQuery.select("K", "Q", "T", "V");
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
            Assert.assertTrue(result.next());
            Map<String, Object> row = result.getRow();
            Assert.assertEquals("ah", new String((byte[]) row.get("K")));
            Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
            Assert.assertEquals(timeStamp, row.get("T"));
            Assert.assertEquals("value1", new String((byte[]) row.get("V")));

            // multiply partition query
            tableQuery = obTableClient.query("testRange");
            tableQuery.addScanRange(new Object[] { "0", "partition".getBytes(), timeStamp },
                                    new Object[] { "xz", "partition".getBytes(), timeStamp });
            tableQuery.select("K", "Q", "T", "V");
            result = tableQuery.execute();
            Assert.assertEquals(4, result.cacheSize());

            // sort result by K
            List<Map<String, Object>> resultList = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(result.next());
                resultList.add(result.getRow());
            }
            Assert.assertFalse(result.next());
            Collections.sort(resultList, new Comparator<Map<String, Object>>() {
                @Override
                public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                    return new String((byte[])o1.get("K")).compareTo(new String((byte[])o2.get("K")));
                }
            });
            String[] orderedKeys = {"0", "ah", "uh", "xh"};
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(orderedKeys[i], new String((byte[])resultList.get(i).get("K")));
                Assert.assertEquals("partition", new String((byte[]) resultList.get(i).get("Q")));
                Assert.assertEquals(timeStamp, resultList.get(i).get("T"));
                Assert.assertEquals("value1", new String((byte[]) resultList.get(i).get("V")));
            }
        } catch (Exception e) {
           e.printStackTrace();
           Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange", new Object[] { "0", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "uh", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testQueryLocalIndex() throws Exception {
        // TODO: client's route with index is wrong
        if (!obTableClient.isOdpMode()) {
           return;
        }
        long timeStamp = System.currentTimeMillis();
        try {
            // the client's route sucks, cannot work in non-odp mode currently
            // p1
            obTableClient.insert("testRange",
                    new Object[] { "a1", "partition".getBytes(), timeStamp + 1 },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testRange",
                    new Object[] { "a1", "partition".getBytes(), timeStamp + 2},
                    new String[] { "V" }, new Object[] { "value3".getBytes() });
            obTableClient.insert("testRange",
                    new Object[] { "a1", "partition".getBytes(), timeStamp + 3},
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            // p2
            obTableClient.insert("testRange",
                    new Object[] { "x1", "partition".getBytes(), timeStamp + 1 },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });

            // single partition query with index
            TableQuery tableQuery = obTableClient.query("testRange");
            tableQuery.addScanRange(new Object[] { "a1", "value1".getBytes() },
                                    new Object[] { "a1", "value9".getBytes() });
            tableQuery.select("K", "Q", "T", "V");
            tableQuery.setScanRangeColumns("K", "V");
            tableQuery.indexName("i1");
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(3, result.cacheSize());

            int[] tsDelta = {1, 3, 2};
            for (int i = 1; i <= 3; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("a1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + tsDelta[i - 1], row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            // query with index backward
            tableQuery.scanOrder(false);
            result = tableQuery.execute();
            Assert.assertEquals(3, result.cacheSize());
            for (int i = 3; i >= 1; i--) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("a1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + tsDelta[i - 1], row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            // multiply partition query with index
            tableQuery = obTableClient.query("testRange");
            tableQuery.addScanRange(new Object[] { "a0", "value1" }, new Object[] { "z9", "value9" } );
            tableQuery.select("K", "Q", "T", "V");
            tableQuery.setScanRangeColumns("K", "V");
            tableQuery.indexName("i1");
            result = tableQuery.execute();
            Assert.assertEquals(4, result.cacheSize());

            // sort result by K, T
            List<Map<String, Object>> resultList = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(result.next());
                resultList.add(result.getRow());
            }
            Assert.assertFalse(result.next());
            Collections.sort(resultList, new Comparator<Map<String, Object>>() {
                @Override
                public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                    int firstCmp = new String((byte[])o1.get("K")).compareTo(new String((byte[])o2.get("K")));
                    if (firstCmp == 0) {
                       return (int)(((long)o1.get("T")) - ((long)o2.get("T")));
                    }
                    return firstCmp;
                }
            });
            String[] orderedKeys = {"a1", "a1", "a1", "x1"};
            int[] orderedDeltaTs = {1, 2, 3, 1};
            String[] orderedValues = {"value1", "value3", "value2", "value1"};
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(orderedKeys[i], new String((byte[])resultList.get(i).get("K")));
                Assert.assertEquals("partition", new String((byte[]) resultList.get(i).get("Q")));
                Assert.assertEquals(timeStamp + orderedDeltaTs[i], resultList.get(i).get("T"));
                Assert.assertEquals(orderedValues[i], new String((byte[]) resultList.get(i).get("V")));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange", new Object[] { "a1", "partition".getBytes(), timeStamp + 1 });
            obTableClient.delete("testRange", new Object[] { "a1", "partition".getBytes(), timeStamp + 2 });
            obTableClient.delete("testRange", new Object[] { "a1", "partition".getBytes(), timeStamp + 3 });
            obTableClient.delete("testRange", new Object[] { "x1", "partition".getBytes(), timeStamp + 1 });
        }
    }

    @Test
    public void testBatch() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch("testRange");
            tableBatchOps.delete(new Object[] { "ah", "partition".getBytes(), timeStamp });
            tableBatchOps.insert(new Object[] { "hh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get("testRange", new Object[] { "ah",
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get("testRange", new Object[] { "xh", "partition".getBytes(),
                    timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("xh", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get("testRange", new Object[] { "xh", "partition".getBytes(),
                    timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("xh", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "hh", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testBatchConcurrent() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        try {
            obTableClient.insert("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch("testRange");
            tableBatchOps.delete(new Object[] { "ah", "partition".getBytes(), timeStamp });
            tableBatchOps.insert(new Object[] { "hh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get("testRange", new Object[] { "ah",
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get("testRange", new Object[] { "xh", "partition".getBytes(),
                    timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("xh", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get("testRange", new Object[] { "xh", "partition".getBytes(),
                    timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("xh", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "hh", "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testBatchConcurrentWithPriority() throws Exception {
        long timeStamp = System.currentTimeMillis();
        ThreadLocalMap.setProcessHighPriority();
        try {
            obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
            obTableClient.insert("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch("testRange");
            tableBatchOps.delete(new Object[] { "ah", "partition".getBytes(), timeStamp });
            tableBatchOps.insert(new Object[] { "hh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(new Object[] { "xh", "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get("testRange", new Object[] { "ah",
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get("testRange", new Object[] { "xh", "partition".getBytes(),
                    timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("xh", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get("testRange", new Object[] { "xh", "partition".getBytes(),
                    timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("xh", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
        } catch (Exception e) {
           e.printStackTrace();
           Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testRange", new Object[] { "ah", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "xh", "partition".getBytes(), timeStamp });
            obTableClient.delete("testRange", new Object[] { "hh", "partition".getBytes(), timeStamp });
        }
    }

}