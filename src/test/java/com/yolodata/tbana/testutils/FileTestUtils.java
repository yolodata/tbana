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

import org.apache.commons.lang.RandomStringUtils;

import java.io.*;
import java.net.URI;

public class FileTestUtils {


    public static String getPathToTestFile(String filename) {
        return TestUtils.TEST_FILE_PATH.concat(filename);
    }

    public static String getRandomTestFilepath() {
        return getPathToTestFile(getRandomFilename());
    }

    public static String getRandomTestFilepath(String extension) {
        return getPathToTestFile(getRandomFilename(extension));
    }

    private static String getRandomFilename() {
        return RandomStringUtils.randomAlphanumeric(25);
    }

    public static String getRandomFilename(String extension) {
        return RandomStringUtils.randomAlphanumeric(25).concat("."+extension);
    }

    public static boolean createFileWithContent(String filepath, String content) throws IOException {
        File f = new File(filepath);
        if(!f.createNewFile())
            return false;

        PrintWriter pw = new PrintWriter(f);
        pw.write(content);
        pw.close();

        return true;
    }

    public static String readContentFromLocalFile(URI path) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(new File(path)));
        String content = org.apache.tools.ant.util.FileUtils.readFully(inputStreamReader);
        inputStreamReader.close();

        return content;
    }
}
