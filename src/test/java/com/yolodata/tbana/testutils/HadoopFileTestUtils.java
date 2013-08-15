/*
 * Copyright (c) 2013 Yolodata, LLC,  All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yolodata.tbana.testutils;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.yolodata.tbana.testutils.FileTestUtils.readContentFromLocalFile;

public class HadoopFileTestUtils {
    public static String readMapReduceOutputFile(FileSystem fs, Path outputPath) throws IOException, IllegalAccessException, InstantiationException {
        FileStatus[] fileStatuses = fs.listStatus(outputPath);
        StringBuilder sb = new StringBuilder();
        for (FileStatus f : fileStatuses) {
            if (f.getPath().toString().endsWith("_SUCCESS"))
                continue; // skip SUCCESS file
            String partFileContent = readContentFromLocalFile(f.getPath().toUri());
            if (partFileContent != null)
                sb.append(partFileContent);
        }

        return sb.toString();
    }

    public static void createFileWithContent(FileSystem fs, Path path, String content) throws IOException {
        FSDataOutputStream fso = fs.create(path, true);
        fso.writeBytes(content);
        fso.flush();
        fso.close();
    }

    public static Path createPath(String directory, String filename) {
        if(!directory.endsWith("/"))
            directory = directory.concat("/");

        String path = directory.concat(filename);

        return new Path(path);
    }

}
