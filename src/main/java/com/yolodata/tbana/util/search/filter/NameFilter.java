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
