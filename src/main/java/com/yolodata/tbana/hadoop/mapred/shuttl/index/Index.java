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

package com.yolodata.tbana.hadoop.mapred.shuttl.index;

import org.apache.hadoop.fs.Path;

import java.io.File;

public class Index {

    private String path;
    private String name;

    public Index(String path) {
        this(path, (new File(path)).getName());
    }

    public Index(Path path) {
        this.path = path.toString();
        this.name = path.getName();
    }

    public Index(String path, String name) {
        this.path = path;
        this.name = name;
    }


    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj == null || obj.getClass() != this.getClass())
            return false;

        Index i = (Index) obj;
        return i.name == this.name && i.path == this.path;
    }
}
