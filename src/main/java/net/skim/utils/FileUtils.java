package net.skim.utils;

import net.skim.exception.ValidatorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.URL;

/**
 * Created by sunghokim on 4/22/2017.
 */
public class FileUtils {
    static final Logger log = LogManager.getLogger(FileUtils.class.getName());

    private String indexFileName;
    private String targetFileName;
    private InputStream indexFileInputStream;
    private InputStream targetFileInputStream;
    private boolean fromResources;
    private ClassLoader loader;

    public FileUtils(String fileOneName, String fileTwoName) throws ValidatorException {
        this(fileOneName, fileTwoName, null);
    }

    public FileUtils(String fileOneName, String fileTwoName, ClassLoader loader) throws ValidatorException {
        if (fileOneName == null || fileTwoName == null) {
            throw new ValidatorException("File names cannot be null");
        }

        if (loader == null) {
            log.debug("ClassLoader is unavailable. It will search files from the arguments instead of the resources");
            fromResources = false;
        } else {
            log.debug("ClassLoader is available. It will search files from the resources");
            fromResources = true;
        }
        this.loader = loader;

        init(fileOneName, fileTwoName);
    }

    private void init(String fileOneName, String fileTwoName) throws ValidatorException {
        indexFileName = null;
        targetFileName = null;
        indexFileInputStream = null;
        targetFileInputStream = null;

        setFileNamesBySize(fileOneName, fileTwoName);
        setIndexFileReader();
        setTargetFileReader();
    }

    private void setFileNamesBySize(String fileOneName, String fileTwoName) throws ValidatorException {
        log.debug(String.format("fileOneName: %s, fileTwoName: %s", fileOneName, fileTwoName));
        try {
            if (getFileSize(fileOneName) <= getFileSize(fileTwoName)) {
                indexFileName = fileOneName;
                targetFileName = fileTwoName;
            } else {
                indexFileName = fileTwoName;
                targetFileName = fileOneName;
            }
        } catch (FileNotFoundException e) {
            throw new ValidatorException(e);
        } catch (IllegalArgumentException | IOException e) {
            log.warn(String.format("Can't calculate file size. The first file will be indexed: %s", e));
            indexFileName = fileOneName;
            targetFileName = fileTwoName;
        }
        log.debug(String.format("indexFileName: %s, targeFileName: %s", indexFileName, targetFileName));
    }

    private void setIndexFileReader() throws ValidatorException {
        indexFileInputStream = getFileInputStream(indexFileName);
    }

    private void setTargetFileReader() throws ValidatorException {
        targetFileInputStream = getFileInputStream(targetFileName);
    }

    public OutputStream getFileOutputStream(String fileName) throws ValidatorException {
        OutputStream ous = null;

        try {
            File file = new File(fileName);
            log.debug(String.format("Trying to delete: %s", file.getAbsolutePath()));
            if (file.exists() && file.isFile()) {
                if (file.delete()) {
                    log.debug(String.format("%s is deleted", fileName));
                } else {
                    log.warn(String.format("Can't delete %s. Old contents may still exist", fileName));
                }
            }

            ous = new FileOutputStream(fileName);
            if (ous == null) {
                throw new FileNotFoundException(String.format("Unable to create the file: %s", fileName));
            }
            log.debug(String.format("%s will be created: %s", fileName, file.getAbsolutePath()));
        } catch (IOException e) {
            throw new ValidatorException(e);
        }
        return ous;
    }

    /**
     * Gets Reader of the given file
     *
     * @param fileName
     * @return Reader of the file. If the file doesn't exist or any errors occurs while accessing the file, returns null
     */
    private InputStream getFileInputStream(String fileName) throws ValidatorException {
        if (fileName == null) {
            throw new ValidatorException("fileName is null");
        }
        InputStream ins = null;
        if (fromResources) {
            try {
                ins = loader.getResourceAsStream(fileName);
                if (ins == null) {
                    throw new FileNotFoundException(String.format("Unable to find the file: %s", fileName));
                }
            } catch (FileNotFoundException e) {
                throw new ValidatorException(e);
            }
        } else {
            try {
                ins = new FileInputStream(fileName);
                if (ins == null) {
                    throw new FileNotFoundException(String.format("Unable to find the file: %s", fileName));
                }
                File file = new File(fileName);
                log.debug(String.format("%s will be read: %s", fileName, file.getAbsolutePath()));
            } catch (FileNotFoundException e) {
                throw new ValidatorException(e);
            }
        }
        return ins;
    }

    public InputStream getIndexFileInputStream() {
        return indexFileInputStream;
    }

    public InputStream getTargetFileInputStream() {
        return targetFileInputStream;
    }

    public String getIndexFileName() {
        return indexFileName;
    }

    public String getTargetFileName() {
        return targetFileName;
    }

    private double getFileSize(String fileName) throws IllegalArgumentException, IOException {
        log.debug(String.format("fileName: %s", fileName));
        double fileSizeByte = 0.0;
        if (fromResources) {
            URL url = loader.getResource(fileName);

            if (url == null) {
                throw new FileNotFoundException("resource not found: " + fileName);
            }

            fileSizeByte = url.openConnection().getContentLength();
            log.debug(String.format("url: %s, size: %f", url.toString(), fileSizeByte));

            if (fileSizeByte < 0) {
                throw new NumberFormatException("File size is bigger than the max integer. Will not check the file size");
            }
        } else {
            File file = new File(fileName);

            if (file.exists() && file.isFile()) {
                fileSizeByte = file.length();
            } else {
                throw new FileNotFoundException("File not found: " + fileName);
            }
        }
        return fileSizeByte;
    }
}
