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

public class ExtensionFilter implements SearchFilter{

    public enum Extension {
        CSV,TSV
    }

    private String extensionString;

    public ExtensionFilter(Extension extension) {
        this.extensionString = getExtensionString(extension);
    }

    public ExtensionFilter(String extension) {
        this.extensionString = extension.startsWith(".") ? extension : ".".concat(extension);
    }

    private String getExtensionString(Extension extension) {
        switch(extension) {
            case CSV: return ".csv";
            case TSV: return ".tsv";
            default:
                throw new ExtensionNotFound("Cannot find the string for extensionString "+extension.toString()+". Try creating the ExtensionFilter using the ExtensionFilter(String) constructor.");
        }
    }

    @Override
    public boolean accept(String path) {
        return path.toLowerCase().endsWith(extensionString.toLowerCase());
    }


    private class ExtensionNotFound extends RuntimeException {
        public ExtensionNotFound(String message) {
            super(message);
        }
    }
}
