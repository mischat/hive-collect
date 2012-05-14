/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jointhegrid.udf.collect;

import com.jointhegrid.hive_test.HiveTestService;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author edward
 */
public class CollectTest extends HiveTestService {

  public CollectTest() throws IOException {
    super();
  }

  public void testCollect() throws Exception {
    Path p = new Path(this.ROOT_DIR, "rankfile");

    FSDataOutputStream o = this.getFileSystem().create(p);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(o));
    bw.write("twelve\t12\n");
    bw.write("twelve\t1\n");
    bw.write("eleven\t11\n");
    bw.write("eleven\t10\n");
    bw.close();

    String jarFile;
    jarFile = GenericUDAFCollect.class.getProtectionDomain().getCodeSource().getLocation().getFile();
    client.execute("add jar " + jarFile);
    client.execute("create temporary function collect as 'com.jointhegrid.udf.collect.GenericUDAFCollect'");
    client.execute("create table  collecttest  (str string, countVal int) row format delimited fields terminated by '09' lines terminated by '10'");
    client.execute("load data local inpath '" + p.toString() + "' into table collecttest");

    client.execute("select collect(str) FROM collecttest");
    List<String> expected = Arrays.asList("[\"twelve\",\"eleven\"]");
    assertEquals(expected, client.fetchAll());


    client.execute("drop table collecttest");
  }
}
