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
        return path.endsWith(extensionString);
    }


    private class ExtensionNotFound extends RuntimeException {
        public ExtensionNotFound(String message) {
            super(message);
        }
    }
}
