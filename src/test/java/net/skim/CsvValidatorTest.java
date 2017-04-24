package net.skim;

import org.junit.After;
import org.junit.Before;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import net.skim.exception.ValidatorException;
import net.skim.utils.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for simple CsvValidator.
 */
public class CsvValidatorTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private File tmpFile;

    private static final String SAMPLE_FILE_ONE = "sample_1.csv";
    private static final String SAMPLE_FILE_TWO = "sample_2.csv";
    private static final String SAMPLE_COMMON_ROW_ONE = "sample_common_rows_index.txt";
    private static final String SAMPLE_COMMON_ROW_TWO = "sample_common_rows_target.txt";

    private ArrayList<String> expectedIndexedIdList;
    private CsvValidator cv;
    private CsvWriter csvWriter;
    private CsvWriterSettings csvWriterSettings;
    private CsvParserSettings csvParserSettings;
    private CsvParser csvParser;
    private OutputStream fos = null;

    @Before
    public void setUp() throws Exception {
        cv = new CsvValidator();
        initCsvUtils();
    }

    @After
    public void tearDown() throws Exception {

    }

    private void initExpectedIndexedIdList() {
        expectedIndexedIdList = new ArrayList<>(7);
        expectedIndexedIdList.add("8036341");
        expectedIndexedIdList.add("8036350");
        expectedIndexedIdList.add("8036351");
        expectedIndexedIdList.add("8036352");
        expectedIndexedIdList.add("8036355");
        expectedIndexedIdList.add("8036359");
        expectedIndexedIdList.add("8036372");
    }

    private void initCsvUtils() throws IOException, ValidatorException {
        csvWriterSettings = new CsvWriterSettings();
        csvWriterSettings.setInputEscaped(true);
        csvWriter = new CsvWriter(csvWriterSettings);

        csvParserSettings = cv.getCsvParserSettings();
        csvParser = new CsvParser(csvParserSettings);

        tmpFile = tempFolder.newFile();
        assertTrue(tmpFile.exists());

        FileUtils fileUtils = new FileUtils(SAMPLE_FILE_ONE, SAMPLE_FILE_TWO, this.getClass().getClassLoader());
        OutputStream fos = fileUtils.getFileOutputStream(tmpFile.getAbsolutePath());
        csvWriter = new CsvWriter(fos, "utf8", csvWriterSettings);
    }

    @Test
    public void testGetIndexedIdList() {
        ArrayList<String> actualIndexedIdList = null;
        try {
            initExpectedIndexedIdList();
            actualIndexedIdList = cv.getIndexedIdList(SAMPLE_FILE_ONE, csvParser, csvWriter, true);
        } catch (IOException e) {
            fail("No exception");
        }
        assertEquals("It needs to be the same", expectedIndexedIdList, actualIndexedIdList);
    }

    @Test
    public void testCompareCommonRows() {
        try {
            boolean result = cv.compareCommonRows(SAMPLE_COMMON_ROW_ONE, SAMPLE_COMMON_ROW_TWO, csvWriter, true);
            assertTrue("It needs to be true", result);
        } catch (IOException e) {
            fail("No exception");
        }
    }
}
