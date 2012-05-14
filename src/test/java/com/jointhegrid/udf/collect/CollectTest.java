/*
Copyright 2012 jointhegrid.com Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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

    client.execute("SELECT concat_ws( ',' , collect(str)) FROM collecttest");
    expected = Arrays.asList("twelve,eleven");
    assertEquals(expected, client.fetchAll());

    client.execute("SELECT str, concat_ws( ',' , collect( cast(countVal as string))) FROM collecttest GROUP BY str");
    expected = Arrays.asList("eleven\t11,10", "twelve\t12,1");
    assertEquals(expected, client.fetchAll());

    client.execute("drop table collecttest");
  }
}
