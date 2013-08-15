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

package com.yolodata.tbana.util.search.filter;

import com.splunk.shuttl.archiver.model.BucketName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class BucketFilter implements SearchFilter {

    private FileSystem fileSystem;

    public BucketFilter(FileSystem fileSystem) {

        this.fileSystem = fileSystem;
    }

    @Override
    public boolean accept(String path) throws IOException {
        DirectoryFilter directoryFilter = new DirectoryFilter(fileSystem);
        return accept(new Path(path), directoryFilter);
    }

    public boolean accept(Path path, SearchFilter dependency) throws IOException {
        if(!dependency.accept(path.toString()))
            return false;

        return validateBucketName(path.getName());
    }

    private boolean validateBucketName(String bucketName) {
        BucketName name = new BucketName(bucketName);
        try{
            // this will run the validate name method on the bucket name
            name.getDB();
        } catch(BucketName.IllegalBucketNameException exception)
        {
            return false;
        }

        return true;
    }
}
