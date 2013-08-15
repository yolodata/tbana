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

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NameFilter implements SearchFilter{

    private String [] names;

    public NameFilter(String name) {

        this.names = new String[] {name};
    }

    public NameFilter(String[] names) {
        this.names = names;
    }

    @Override
    public boolean accept(String path) throws IOException {
        Path p = new Path(path);

        return matchesAnyName(p.getName());
    }

    private boolean matchesAnyName(String name) {
        if(names.length == 0)
            return true;

        for(String n : names) {
            // add regex if string contains *
            if(containsWildcard(n))
               return doRegexMatching(n, name);
            Pattern.compile(name);
            if(n.equals(name))
                return true;
        }

        return false;
    }

    private boolean doRegexMatching(String namePattern, String nameToMatch) {

        namePattern = namePattern.replace("*","(.*)");
        namePattern = ("^".concat(namePattern)).concat("$");
        Pattern p = Pattern.compile(namePattern);
        Matcher m = p.matcher(nameToMatch);
        return m.matches();
    }

    private boolean containsWildcard(String name) {
        return name.contains("*");
    }


}
