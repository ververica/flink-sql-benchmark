/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.benchmark;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URI;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public class GenTable extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new GenTable(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] remainingArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    CommandLineParser parser = new BasicParser();
    getConf().setInt("io.sort.mb", 4);
    org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
    options.addOption("s", "scale", true, "scale");
    options.addOption("t", "table", true, "table");
    options.addOption("d", "dir", true, "dir");
    options.addOption("p", "parallel", true, "parallel");
    CommandLine line = parser.parse(options, remainingArgs);

    if (!(line.hasOption("scale") && line.hasOption("dir"))) {
      HelpFormatter f = new HelpFormatter();
      f.printHelp("GenTable", options);
      return 1;
    }

    int scale = Integer.parseInt(line.getOptionValue("scale"));
    String table = line.getOptionValue("table", "all");
    Path out = new Path(line.getOptionValue("dir"));

    int parallel = scale;

    if (line.hasOption("parallel")) {
      parallel = Integer.parseInt(line.getOptionValue("parallel"));
    }

    if (parallel == 1 || scale == 1) {
      System.err.println("The MR task does not work for scale=1 or parallel=1");
      return 1;
    }

    Path in = genInput(table, scale, parallel);

    Path dsdgen = copyJar(new File("target/lib/dsdgen.jar"));
    URI dsuri = dsdgen.toUri();
    URI link = new URI(dsuri.getScheme(),
        dsuri.getUserInfo(), dsuri.getHost(),
        dsuri.getPort(), dsuri.getPath(),
        dsuri.getQuery(), "dsdgen");
    Configuration conf = getConf();
    conf.setInt("mapred.task.timeout", 0);
    conf.setInt("mapreduce.task.timeout", 0);
    conf.setBoolean("mapreduce.map.output.compress", true);
    conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
    DistributedCache.addCacheArchive(link, conf);
    DistributedCache.createSymlink(conf);
    Job job = new Job(conf, "GenTable+" + table + "_" + scale);
    job.setJarByClass(getClass());
    job.setNumReduceTasks(0);
    job.setMapperClass(DSDGen.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.setNumLinesPerSplit(job, 1);

    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);

    // use multiple output to only write the named files
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "text",
        TextOutputFormat.class, LongWritable.class, Text.class);

    boolean success = job.waitForCompletion(true);

    // cleanup
    FileSystem fs = FileSystem.get(getConf());

    fs.delete(in, false);
    fs.delete(dsdgen, false);

    return 0;
  }

  private Path copyJar(File jar) throws Exception {
    MessageDigest md = MessageDigest.getInstance("MD5");
    InputStream is = new FileInputStream(jar);
    try {
      is = new DigestInputStream(is, md);
      // read stream to EOF as normal...
    } finally {
      is.close();
    }
    BigInteger md5 = new BigInteger(md.digest());
    String md5hex = md5.toString(16);
    Path dst = new Path(String.format("/tmp/%s.jar", md5hex));
    Path src = new Path(jar.toURI());
    FileSystem fs = FileSystem.get(getConf());
    fs.copyFromLocalFile(false, /*overwrite*/true, src, dst);
    return dst;
  }

  private Path genInput(String table, int scale, int parallel) throws Exception {
    long epoch = System.currentTimeMillis() / 1000;

    Path in = new Path("/tmp/" + table + "_" + scale + "-" + epoch);
    FileSystem fs = FileSystem.get(getConf());
    FSDataOutputStream out = fs.create(in);
    for (int i = 1; i <= parallel; i++) {
      if (table.equals("all")) {
        out.writeBytes(String
            .format("./dsdgen -dir $DIR -force Y -scale %d -parallel %d -child %d\n", scale,
                parallel, i));
      } else {
        out.writeBytes(String
            .format("./dsdgen -dir $DIR -table %s -force Y -scale %d -parallel %d -child %d\n",
                table, scale, parallel, i));
      }
    }
    out.close();
    return in;
  }

  private static String readToString(InputStream in) throws IOException {
    InputStreamReader is = new InputStreamReader(in);
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader(is);
    String read = br.readLine();

    while (read != null) {
      //System.out.println(read);
      sb.append(read);
      read = br.readLine();
    }
    return sb.toString();
  }

  private static final class DSDGen extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs mos;

    protected void setup(Context context) throws IOException {
      mos = new MultipleOutputs(context);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }

    protected void map(LongWritable offset, Text command, Mapper.Context context)
        throws IOException, InterruptedException {
      String parallel = "1";
      String child = "1";

      String[] cmd = command.toString().split(" ");

      for (int i = 0; i < cmd.length; i++) {
        if (cmd[i].equals("$DIR")) {
          cmd[i] = (new File(".")).getAbsolutePath();
        }
        if (cmd[i].equals("-parallel")) {
          parallel = cmd[i + 1];
        }
        if (cmd[i].equals("-child")) {
          child = cmd[i + 1];
        }
      }

      Process p = Runtime.getRuntime().exec(cmd, null, new File("dsdgen/tools/"));
      int status = p.waitFor();
      if (status != 0) {
        String err = readToString(p.getErrorStream());
        throw new InterruptedException("Process failed with status code " + status + "\n" + err);
      }

      File cwd = new File(".");
      final String suffix = String.format("_%s_%s.dat", child, parallel);

      FilenameFilter tables = (dir, name) -> name.endsWith(suffix);

      for (File f : cwd.listFiles(tables)) {
        BufferedReader br = new BufferedReader(new FileReader(f));
        String line;
        while ((line = br.readLine()) != null) {
          // process the line.
          mos.write("text", line, null, f.getName().replace(suffix, "/data"));
        }
        br.close();
        f.deleteOnExit();
      }
    }
  }
}
