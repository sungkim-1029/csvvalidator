package net.skim;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import net.skim.exception.ValidatorException;
import net.skim.utils.FileUtils;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.annotation.Arg;
import net.sourceforge.argparse4j.inf.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A validator for CSV files. It compares two files and stores the difference in a different CSV file
 * A function excluding columns is not implemented yet.
 * <p>
 * First of all, it creates an index of the smaller file. After that, it walks through the target file, which is larger,
 * to looking for the different rows. If they are found, they are saved in the result file. If each ID of the rows is the same
 * with that in the list, it creates another list of common rows and stores related rows in a file, "common_rows_target.txt".
 * The, it walks through the index file again with the list of common IDs. It save the rows which are not in the list
 * to the result file and stores the rows related with IDs in a file, "common_rows_index.txt", to find the differences
 * between two files about common rows. If the differences are found between two rows, they are saved in the result.
 */
public class CsvValidator {
    static final Logger log = LogManager.getLogger(CsvValidator.class.getName());

    private static final String RESULT_FILE_NAME = "result_diff.csv";

    private CsvParserSettings csvParserSettings;

    public CsvValidator() {
        csvParserSettings = new CsvParserSettings();
        csvParserSettings.setKeepQuotes(true);
        csvParserSettings.setHeaderExtractionEnabled(true);
    }

    public CsvParserSettings getCsvParserSettings() {
        return csvParserSettings;
    }

    /**
     * It retuns a parser for command-line arguments
     *
     * @return a argument parser
     */
    private static ArgumentParser getArgParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser(CsvValidator.class.getName());
        parser.addArgument("fileName")
                .nargs(2)
                .help("CSV file to be compared");
        parser.addArgument("-o", "--out")
                .help("Destination file")
                .setDefault(RESULT_FILE_NAME);
        parser.addArgument("-ex", "--exclude")
                .type(new PossibleColumn())
                .nargs("*")
                .help("Excludes the columns when comparing two files. The first column (the primary key), ID, is not available");
        return parser;
    }

    /**
     * It walks through the target file with the indexedIdList, and makes another list containing common IDs, which is
     * used to find rows of the index file. It also sends rows not in the indexedIdList to the result file, and saves
     * common rows matching with the common IDs into a common_rows_target.txt file. These rows are compared with those
     * of the indexed file, and saved in the result file if they are not same.
     *
     * @param targetFileName
     * @param csvWriter
     * @param indexedIdList
     * @return
     * @throws IOException
     */
    public ArrayList<String> readTargetAndWriteAsync(String targetFileName, String commonRowsFile, CsvParser csvParser, CsvWriter csvWriter, ArrayList<String> indexedIdList) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(10);
        ArrayList<String> commonIdList = new ArrayList<String>(10000);

        boolean header = true;
        InputStream is = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String[] parsedHeader = null;
        BufferedWriter bw = new BufferedWriter(new FileWriter(commonRowsFile));
        try {
            is = new FileInputStream(targetFileName);
            isr = new InputStreamReader(is);
            br = new BufferedReader(isr);

            String stringLine = null;
            Record rowRecord = null;
            log.debug("Processing: " + targetFileName);
            while ((stringLine = br.readLine()) != null) {
                if (header) {
                    header = false;
                    csvParser.parseLine(stringLine);
                    continue;
                }

                rowRecord = csvParser.parseRecord(stringLine);
                if (rowRecord == null) {
                    throw new ValidatorException("rowRecord is null");
                }

                String id = rowRecord.getString("ID");
                if (indexedIdList.contains(id)) {
                    commonIdList.add(id);
                    Runnable commonRowWriterWorker = new CommonRowWriterRunner(bw, Arrays.toString(rowRecord.getValues()));
                    executor.execute(commonRowWriterWorker);
                } else {
                    log.debug(Arrays.toString(rowRecord.getValues()));
                    Runnable resultWriterWorker = new CsvWriterForStringRunner(csvWriter, Arrays.toString(rowRecord.getValues()));
                    executor.execute(resultWriterWorker);
                }
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
            log.debug("Processing done: " + targetFileName);

        } catch (IOException | ValidatorException e) {
            System.exit(1);
        } finally {
            try {
                if (bw != null) bw.close();
                if (is != null) is.close();
                if (isr != null) isr.close();
                if (br != null) br.close();
            } catch (IOException e) {
                // closing
            }
        }
        return commonIdList;
    }

    /**
     * It walks through the index file with the list of the common IDs, and sends rows not in the commonIdList
     * to the result file, and saves common rows matching with the common IDs into a common_rows_index.txt file.
     * These rows are compared with those of the target file, and saved in the result file if they are not same.
     *
     * @param indexFileName
     * @param csvWriter
     * @param commonIdList
     * @throws IOException
     */
    public void readIndexAndWriteAsync(String indexFileName, String commonRowsFile, CsvParser csvParser, CsvWriter csvWriter, ArrayList<String> commonIdList) throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(10);

        boolean header = true;
        InputStream is = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        BufferedWriter bw = new BufferedWriter(new FileWriter(commonRowsFile));
        try {
            is = new FileInputStream(indexFileName);
            isr = new InputStreamReader(is);
            br = new BufferedReader(isr);

            String stringLine = null;
            Record rowRecord = null;
            log.debug("Processing: " + indexFileName);
            while ((stringLine = br.readLine()) != null) {
                if (header) {
                    header = false;
                    csvParser.parseLine(stringLine);
                    continue;
                }

                rowRecord = csvParser.parseRecord(stringLine);
                if (rowRecord == null) {
                    throw new ValidatorException("rowRecord is null");
                }

                String id = rowRecord.getString("ID");
                if (commonIdList.contains(id)) {
                    Runnable commonRowWriterWorker = new CommonRowWriterRunner(bw, Arrays.toString(rowRecord.getValues()));
                    executor.execute(commonRowWriterWorker);
                } else {
                    Runnable resultWriterWorker = new CsvWriterForStringRunner(csvWriter, Arrays.toString(rowRecord.getValues()));
                    executor.execute(resultWriterWorker);
                }
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
            log.debug("Processing done: " + indexFileName);

        } catch (IOException | ValidatorException e) {
            System.exit(1);
        } finally {
            try {
                if (bw != null) bw.close();
                if (is != null) is.close();
                if (isr != null) isr.close();
                if (br != null) br.close();
            } catch (IOException e) {
                // closing
            }
        }
    }

    /**
     * It compares common rows in two files and saves the differences in the result file
     *
     * @param csvWriter
     * @throws IOException
     */
    public boolean compareCommonRows(String commonRowsFile1, String commonRowsFile2, CsvWriter csvWriter, boolean fromResources) throws IOException {
        boolean result = true;
        ExecutorService executor = Executors.newFixedThreadPool(10);

        BufferedReader br1 = null;
        BufferedReader br2 = null;
        InputStream is1 = null;
        InputStream is2 = null;
        try {
            if (fromResources) {
                is1 = Thread.currentThread().getContextClassLoader().getResourceAsStream(commonRowsFile1);
                is2 = Thread.currentThread().getContextClassLoader().getResourceAsStream(commonRowsFile2);
            } else {
                is1 = new FileInputStream(commonRowsFile1);
                is2 = new FileInputStream(commonRowsFile2);
            }

            // Create the object of BufferedReader object
            br1 = new BufferedReader(new InputStreamReader(is1));
            br2 = new BufferedReader(new InputStreamReader(is2));

            String stringLine1 = br1.readLine();
            String stringLine2 = br2.readLine();

            log.debug("Comparing two files");
            while ((stringLine1 != null || (stringLine2 != null))) {
                if (stringLine1 == null || stringLine2 == null) {
                    if (stringLine1 != null) {
                        result = false;
                        Runnable resultWriterWorker = new CsvWriterForStringRunner(csvWriter, stringLine1);
                        executor.execute(resultWriterWorker);
                    } else {
                        result = false;
                        Runnable resultWriterWorker = new CsvWriterForStringRunner(csvWriter, stringLine2);
                        executor.execute(resultWriterWorker);
                    }
                } else if (!stringLine1.equals(stringLine2)) {
                    result = false;
                    Runnable resultWriterWorker = new CsvWriterForStringRunner(csvWriter, stringLine1, stringLine2);
                    executor.execute(resultWriterWorker);
                } else {
                    // Do nothing
                }

                stringLine1 = br1.readLine();
                stringLine2 = br2.readLine();
            }
            executor.shutdown();
            while (!executor.isTerminated()) {
            }
            log.debug("Comparing done");

        } catch (NullPointerException | IOException e) {
            log.error(e.getMessage(), Arrays.toString(e.getStackTrace()));
            System.exit(1);
        } finally {
            try {
                if (br1 != null) br1.close();
                if (br2 != null) br2.close();
                if (is1 != null) is1.close();
                if (is2 != null) is2.close();
            } catch (IOException e) {
                // closing
            }
        }
        return result;
    }

    public void compareCommonRows(String commonRowsFile1, String commonRowsFile2, CsvWriter csvWriter) throws IOException {
        compareCommonRows(commonRowsFile1, commonRowsFile2, csvWriter, false);
    }

    /**
     * It creates a list of IDs in the index file. While walking though the smaller file, it saves the whole IDs in the
     * file
     *
     * @param indexFileName
     * @param csvParser
     * @param csvWriter
     * @return
     * @throws IOException
     */
    public ArrayList<String> getIndexedIdList(String indexFileName, CsvParser csvParser, CsvWriter csvWriter, boolean fromResources) throws IOException {
        ArrayList<String> indexedIdList = new ArrayList<String>();

        InputStream is = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        boolean header = true;

        if (fromResources) {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(indexFileName);
        } else {
            is = new FileInputStream(indexFileName);
        }

        isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        String stringLine = null;
        String[] parsedLine = null;

        log.debug("Indexing: " + indexFileName);
        while ((stringLine = br.readLine()) != null) {
            if (header) {
                header = false;
                csvParser.parseLine(stringLine);

                // Saves the header in the result file
                csvWriter.writeRow(stringLine);
                continue;
            }

            parsedLine = csvParser.parseLine(stringLine);
            indexedIdList.add(parsedLine[0]);
        }
        log.debug("Indexing done: " + indexFileName);
        try {
            if (is != null) is.close();
            if (isr != null) isr.close();
            if (br != null) br.close();
        } catch (IOException e) {
            // closing
        }

        return indexedIdList;
    }

    public ArrayList<String> getIndexedIdList(String indexFileName, CsvParser csvParser, CsvWriter csvWriter) throws IOException {
        return getIndexedIdList(indexFileName, csvParser, csvWriter, false);
    }

    public static void main(String[] args) {
        // Gets arguments
        ArgumentParser parser = getArgParser();
        ArgParserOption opt = new ArgParserOption();
        try {
            parser.parseArgs(args, opt);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            parser.printHelp();
            System.exit(1);
        }

        CsvValidator cv = new CsvValidator();

        // Init CSV writer
        CsvWriter csvWriter = null;
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        csvWriterSettings.setInputEscaped(true);
        CsvParserSettings csvParserSettings = cv.csvParserSettings;
        CsvParser csvParser = new CsvParser(csvParserSettings);

        FileUtils fileUtils = null;
        OutputStream fos = null;
        ArrayList<String> indexedIdList = null;
        ArrayList<String> commonIdList = null;
        try {
            fileUtils = new FileUtils(opt.fileName.get(0), opt.fileName.get(1));

            fos = fileUtils.getFileOutputStream(opt.out);
            csvWriter = new CsvWriter(fos, "utf8", csvWriterSettings);

            indexedIdList = cv.getIndexedIdList(fileUtils.getIndexFileName(), csvParser, csvWriter);
            commonIdList = cv.readTargetAndWriteAsync(fileUtils.getTargetFileName(), fileUtils.getCommonRowsInTargetFile(), csvParser, csvWriter, indexedIdList);
            cv.readIndexAndWriteAsync(fileUtils.getIndexFileName(), fileUtils.getCommonRowsInIndexFile(), csvParser, csvWriter, commonIdList);
            cv.compareCommonRows(fileUtils.getCommonRowsInIndexFile(), fileUtils.getCommonRowsInTargetFile(), csvWriter);
        } catch (IOException | ValidatorException e) {
            log.error(e);
            System.exit(1);
        }

        csvWriter.close();
    }


    /**
     * It is for guarding argument option -ex, which excludes columns when reading CSV files
     * Currently, not used because the source code is not implemented yet.
     */
    private static class PossibleColumn implements ArgumentType<String> {
        @Override
        public String convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
            try {
                String columnName = value;
                if (columnName == null || "ID".equals(columnName)) {
                    throw new IllegalArgumentException("The first column, ID, cannot be excluded");
                }
                return columnName;
            } catch (IllegalArgumentException e) {
                throw new ArgumentParserException(e, parser);
            }
        }
    }

    /**
     * A model class representing command-line arguments
     * It is used by {@link ArgumentParser}
     */
    private static class ArgParserOption {
        @Arg(dest = "fileName")
        public List<String> fileName;

        @Arg(dest = "out")
        public String out;

        @Arg(dest = "exclude")
        public ArrayList<String> columnNames;
    }

    /**
     * It is a runner for {@link ExecutorService}. It writes common rows into
     * a specific file via {@link BufferedWriter}. The written common rows in
     * two files are compared if they are the same, and the differences are saved
     * in to the result file by {@link CommonRowWriterRunner}
     *
     * @see CsvValidator#readIndexAndWriteAsync(String, String, CsvParser, CsvWriter, ArrayList)
     * @see CsvValidator#readTargetAndWriteAsync(String, String, CsvParser, CsvWriter, ArrayList)
     */
    private static class CommonRowWriterRunner implements Runnable {

        private BufferedWriter bw;
        private String stringLine;

        public CommonRowWriterRunner(BufferedWriter bw, String stringLine) {
            this.bw = bw;
            this.stringLine = stringLine;
        }

        @Override
        public void run() {
            try {
                bw.write(stringLine);
                bw.newLine();
            } catch (IOException e) {
                log.error("Failed to write common lines", e);
            }
        }
    }

    /**
     * It is a runner for {@link ExecutorService}. It writes common rows into
     * a specific file via {@link BufferedWriter}. The written common rows in
     * two files are compared if they are the same, and the differences are saved
     * in to the result file by {@link CommonRowWriterRunner}
     *
     * @see CsvValidator#readIndexAndWriteAsync(String, String, CsvParser, CsvWriter, ArrayList)
     * @see CsvValidator#readTargetAndWriteAsync(String, String, CsvParser, CsvWriter, ArrayList)
     * @see CsvValidator#compareCommonRows(String, String, CsvWriter)
     */
    private static class CsvWriterForStringRunner implements Runnable {

        private CsvWriter csvWriter;
        private String stringLine1;
        private String stringLine2;

        public CsvWriterForStringRunner(CsvWriter csvWriter, String stringLine) {
            this(csvWriter, stringLine, null);
        }

        public CsvWriterForStringRunner(CsvWriter csvWriter, String stringLine1, String stringLine2) {
            this.csvWriter = csvWriter;
            this.stringLine1 = stringLine1;
            this.stringLine2 = stringLine2;
        }

        @Override
        public void run() {
            csvWriter.writeRow(stringLine1.replaceAll("\\[", "").replaceAll("\\]", ""));
            if (stringLine2 != null) {
                csvWriter.writeRow(stringLine2.replaceAll("\\[", "").replaceAll("\\]", ""));
            }
        }
    }

}
