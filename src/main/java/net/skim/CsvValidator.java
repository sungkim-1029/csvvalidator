package net.skim;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.*;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.conversions.Conversions;
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

/**
 * Initial commit
 */
public class CsvValidator {
    static final Logger log = LogManager.getLogger(CsvValidator.class.getName());

    private static final String FILE_NAME_ONE = "sample_1.csv";
    private static final String FILE_NAME_TWO = "sample_2.csv";
    private static final String RESULT_FILE_NAME = "result_diff.csv";

    private CsvParserSettings parserSettings;
//    private CsvParser csvParser;
    private ClassLoader loader;
    private RowListProcessor rowProcessor;

    public CsvValidator(boolean fromResources) {
        rowProcessor = new RowListProcessor();
        parserSettings = new CsvParserSettings();
        parserSettings.setKeepQuotes(true);
        parserSettings.setHeaderExtractionEnabled(true);
//        csvParser = new CsvParser(parserSettings);

        if (fromResources) {
            loader = Thread.currentThread().getContextClassLoader();
        } else {
            loader = null;
        }
    }

    public CsvValidator() {
        this(false);
    }

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

    private static class ArgParserOption {
        @Arg(dest = "fileName")
        public List<String> fileName;

        @Arg(dest = "out")
        public String out;

        @Arg(dest = "exclude")
        public ArrayList<String> columnNames;
    }

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

    public static void main(String[] args) {
        ArgumentParser parser = getArgParser();
        ArgParserOption opt = new ArgParserOption();
        try {
            parser.parseArgs(args, opt);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            parser.printHelp();
            System.exit(1);
        }

        ArrayList<String> columnIndexExcluded = opt.columnNames;

        OutputStream fos;
        ArrayList<String> header = null;
        CsvWriter csvWriter = null;
        CsvValidator cv = new CsvValidator(false);

//        CsvParserSettings csvParserSettings = cv.parserSettings;
//        CsvParser csvParser = new CsvParser(csvParserSettings);

        RowListProcessor rowProcessor = cv.rowProcessor;
        CsvParserSettings csvParserSettingsWithRowProcessor = new CsvParserSettings();
        csvParserSettingsWithRowProcessor.setKeepQuotes(true);
        csvParserSettingsWithRowProcessor.setHeaderExtractionEnabled(true);
        csvParserSettingsWithRowProcessor.setProcessor(new ConcurrentRowProcessor(rowProcessor, 5000));
        CsvParser csvParserWithRowProcessor = new CsvParser(csvParserSettingsWithRowProcessor);
//        ClassLoader loader = cv.loader;
        CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
        csvWriterSettings.setInputEscaped(true);

        Map<String, ArrayList<String>> indexedMap = new LinkedHashMap<String, ArrayList<String>>();

        try {
            FileUtils fileUtils = new FileUtils(opt.fileName.get(0), opt.fileName.get(1));
            fos = fileUtils.getFileOutputStream(opt.out);

            csvWriter = new CsvWriter(fos, "utf8", csvWriterSettings);

            log.debug(String.format("Indexing %s", fileUtils.getIndexFileName()));
            List<String[]> indexFileRows;
            if (columnIndexExcluded != null && !columnIndexExcluded.isEmpty()) {
                String[] columnExcludedArray = new String[columnIndexExcluded.size()];
                csvParserSettingsWithRowProcessor.excludeFields(columnIndexExcluded.toArray(columnExcludedArray));
                csvParserWithRowProcessor = new CsvParser(csvParserSettingsWithRowProcessor);

//                csvWriter.writeHeaders(csvParser.getContext().parsedHeaders());
//                indexFileRows = csvParser.parseAll(fileUtils.getIndexFileReader());
            } else {
//                csvWriter.writeHeaders(csvParser.getContext().parsedHeaders());
//                indexFileRows = csvParser.parseAll(fileUtils.getIndexFileReader());
            }

//            indexFileRows = csvParser.parseAll(fileUtils.getIndexFileReader());
//            csvWriter.writeHeaders(csvParser.getContext().parsedHeaders());
            csvParserWithRowProcessor.parse(fileUtils.getIndexFileInputStream(), "utf8");
            indexFileRows = rowProcessor.getRows();
            csvWriter.writeHeaders(rowProcessor.getHeaders());

            ArrayList<String> rowArrayList;
            for (String[] row : indexFileRows) {
                rowArrayList = new ArrayList<>(row.length);
                rowArrayList.addAll(Arrays.asList(row));
                indexedMap.put(rowArrayList.get(0), rowArrayList);
            }
//
//            csvParser.beginParsing(fileUtils.getIndexFileReader());
//            Record recordRow;
//            while ((recordRow = csvParser.parseNextRecord()) != null) {
//                // Assume that the first line is the header and starts with "ID", which is used as a primary key
//                // Assume that the header of the indexFile is the same with that of the targetFile
//                // Assume that the number of columns in a row is the same with that in the header
//                if (columnIndexExcluded != null && !columnIndexExcluded.isEmpty()) {
//                    if (header == null || header.isEmpty()) {
//                        header = new ArrayList<>();
//                        header.addAll(Arrays.asList(csvParser.getContext().parsedHeaders()));
//                        header.removeAll(columnIndexExcluded);
//                        numLines++;
//                    }
//
//                    ArrayList<String> modifiedRow = new ArrayList<>();
//                    for (String column : header) {
//                        if (!columnIndexExcluded.contains(column)) {
//                            modifiedRow.add(recordRow.getString(column));
//                        }
//                    }
//                    indexedMap.put(modifiedRow.get(0), modifiedRow);
//                    numLines++;
//                } else {
//                    if (header == null || header.isEmpty()) {
//                        header = new ArrayList<>();
//                        header.addAll(Arrays.asList(csvParser.getContext().parsedHeaders()));
//                        numLines++;
//                    }
//
//                    ArrayList<String> row = new ArrayList<>();
//                    row.addAll(Arrays.asList(recordRow.getValues()));
//
//                    indexedMap.put(row.get(0), row);
//                    numLines++;
//                }
//            }
//            csvParser.stopParsing();
//            log.debug("Number of Lines of indexFile: " + numLines);

//            csvWriter.writeHeaders(header);

            log.debug(String.format("Processing %s", fileUtils.getTargetFileName()));
            List<Record> targetFileRowRecords;
            CsvParserSettings csvParserSettings = cv.parserSettings;
            CsvParser csvParser = new CsvParser(csvParserSettings);

            if (columnIndexExcluded != null && !columnIndexExcluded.isEmpty()) {
                String[] columnExcludedArray = new String[columnIndexExcluded.size()];
                csvParserSettings.excludeFields(columnIndexExcluded.toArray(columnExcludedArray));
                csvParser = new CsvParser(csvParserSettings);
            }
            targetFileRowRecords = csvParser.parseAllRecords(fileUtils.getTargetFileInputStream(), "utf8");
//            if (columnIndexExcluded != null && !columnIndexExcluded.isEmpty()) {
////                String[] columnExcludedArray = new String[columnIndexExcluded.size()];
////                csvParserSettings.excludeFields(columnIndexExcluded.toArray(columnExcludedArray));
////                csvParser = new CsvParser(csvParserSettings);
//                // csvParser came from above
//                targetFileRowRecords = csvParser.parseAllRecords(fileUtils.getTargetFileReader());
//            } else {
//                targetFileRowRecords = csvParser.parseAllRecords(fileUtils.getTargetFileReader());
//            }

            ArrayList<String> modifiedRow;
            for (Record rowRecord : targetFileRowRecords) {
                String[] row = rowRecord.getValues();
                modifiedRow = new ArrayList<>(row.length);
                modifiedRow.addAll(Arrays.asList(row));

                String id = rowRecord.getString("ID");
                if (indexedMap.containsKey(id)) {
                    if (indexedMap.get(id).equals(modifiedRow)) {
                        indexedMap.remove(id);
                    }
                } else {
                    // Save target to out
                    log.info(String.format("[Target] - %s", modifiedRow));
                    csvWriter.writeRow(modifiedRow.toArray());
                }
            }
//            numLines = 1;   //Skipping the header
//
//            csvParser.beginParsing(fileUtils.getTargetFileReader());
//
//            while ((recordRow = csvParser.parseNextRecord()) != null) {
//                // Assume that the first line is the header and starts with "ID", which is used as a primary key
//                // Assume that the header of the indexFile is the same with that of the targetFile
//                // Assume that the number of columns in a row is the same with that in the header
//                if (columnIndexExcluded != null && !columnIndexExcluded.isEmpty()) {
//
//                    ArrayList<String> modifiedRow = new ArrayList<>();
//                    for (String column : header) {
//                        if (!columnIndexExcluded.contains(column)) {
//                            modifiedRow.add(recordRow.getString(column));
//                        }
//                    }
//
//                    String id = recordRow.getString("ID");
//                    if (indexedMap.containsKey(id)) {
//                        if (indexedMap.get(id).equals(modifiedRow)) {
//                            indexedMap.remove(id);
//                        }
//                    } else {
//                        // Save target to out
//                        log.info(String.format("[Target] - %s", modifiedRow));
//                        csvWriter.writeRow(modifiedRow.toArray());
//                    }
//                    numLines++;
//
//                } else {
//                    ArrayList<String> row = new ArrayList<>();
//                    row.addAll(Arrays.asList(recordRow.getValues()));
//
//                    String id = recordRow.getString("ID");
//                    if (indexedMap.containsKey(id)) {
//                        if (indexedMap.get(id).equals(row)) {
//                            indexedMap.remove(id);
//                        }
//                    } else {
//                        // Save target to out
//                        log.info(String.format("[Target] - %s", row));
//                        csvWriter.writeRow(row.toArray());
//                    }
//                    numLines++;
//                }
//            }
//            csvParser.stopParsing();
//            log.debug("Number of Lines of targetFile: " + numLines);
        } catch (NullPointerException | ValidatorException e) {
            log.error(e.getStackTrace(), e);
            System.exit(1);
        }

        Iterator<HashMap.Entry<String, ArrayList<String>>> entries = indexedMap.entrySet().iterator();
        List<String> row = null;
        while (entries.hasNext()) {
            HashMap.Entry<String, ArrayList<String>> entry = entries.next();
            row = entry.getValue();
            log.info(String.format("[Index] - %s", row));
            csvWriter.writeRow(row.toArray());
        }

        csvWriter.close();
    }
}
