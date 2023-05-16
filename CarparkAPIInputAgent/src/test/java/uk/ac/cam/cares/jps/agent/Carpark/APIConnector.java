package uk.ac.cam.cares.jps.agent.Carpark;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import uk.ac.cam.cares.jps.agent.Carpark.APIConnector;

import org.json.JSONObject;
import org.junit.*;
import org.junit.rules.TemporaryFolder;


import static com.github.tomakehurst.wiremock.client.WireMock.*;


import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class APIConnectorTest {
    // Temporary folder to place a properties file
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // Fields used for the mock API calls
    private static final int PORT = 8089;
    private static final String TEST_URL = "http://localhost:" + PORT + "/";
    // Mocking objects to mock weather station API calls
    @Rule
    public WireMockRule carparkAPIMock = new WireMockRule(PORT);

    private APIConnectorTest testConnector;

    @Before
    public void initializeTestConnector() {
        testConnector = new APIConnector(TEST_URL,"password", "id", TEST_URL);
    }

    @After
    public void resetAPIMock() {
        carparkAPIMock.resetAll();
    }

    @Test
    public void caparkAPIConnectorConstructorTest() throws NoSuchFieldException, IllegalAccessException, IOException
    {
        // One connector constructed using the password, id and url directly
        APIConnector connector = new APIConnector("password", "id", "url");
        // One connector constructed using a properties file
        String propertiesFile = Paths.get(folder.getRoot().toString(), "api.properties").toString();
        writePropertyFile(propertiesFile, Arrays.asList("carpark.api_url=url", "carpark.accountKey=key"));
        APIConnector connectorFile = new APIConnector(propertiesFile);

        // Retrieve private fields for accountKey and api_url. Check that they were set correctly
        Field apiAccountKey = APIConnector.class.getDeclaredField("accountKey");
        apiAccountKey.setAccessible(true);
        Assert.assertEquals("password", apiAccountKey.get(connector));
        Assert.assertEquals("password", apiAccountKey.get(connectorFile));

        Field urlField = APIConnector.class.getDeclaredField("api_url");
        urlField.setAccessible(true);
        Assert.assertEquals("url", urlField.get(connector));
        Assert.assertEquals("url", urlField.get(connectorFile));
    }

    @Test
    public void loadAPIConfigsTest() throws NoSuchMethodException, IllegalAccessException, IOException, NoSuchFieldException
    {
        // Filepath to not yet created file in temporary test folder
        String filepath = Paths.get(folder.getRoot().toString(), "carpark.properties").toString();
        // Error messages
        String fileNotFound = "There was no properties file found in the specified path: " + filepath;
        String noAPIKey = "The properties file is missing \"carpark.accountKey=<accountKey>\"";
        String noURL = "The properties file is missing \"carpark.api_url=<api_url>\"";

        // Set private method to be accessible
        Method loadAPIConfig = APIConnector.class.getDeclaredMethod("loadAPIconfigs", String.class);
        loadAPIConfig.setAccessible(true);

        // Test for non-existing properties file
        try {
            loadAPIConfig.invoke(testConnector, filepath);
            Assert.fail();
        } catch (InvocationTargetException e) {
            Assert.assertEquals(FileNotFoundException.class, e.getCause().getClass());
            Assert.assertEquals(fileNotFound, e.getCause().getMessage());
        }

        // Test for missing accountKey by creating a file only containing url
        writePropertyFile(filepath, Collections.singletonList("carpark.api_url=url"));
        // Try loading RDB configs
        try {
            loadAPIConfig.invoke(testConnector, filepath);
            Assert.fail();
        } catch (InvocationTargetException e) {
            Assert.assertEquals(IOException.class, e.getCause().getClass());
            Assert.assertEquals(noAPIKey, e.getCause().getMessage());
        }

        // Test for missing url by creating a file only containing accountKey
        writePropertyFile(filepath, Collections.singletonList("carpark.accountKey=key"));
        // Try loading RDB configs
        try {
            loadAPIConfig.invoke(testConnector, filepath);
            Assert.fail();
        } catch (InvocationTargetException e) {
            Assert.assertEquals(IOException.class, e.getCause().getClass());
            Assert.assertEquals(noURL, e.getCause().getMessage());
        }

        // Test for proper accountKey and url
        writePropertyFile(filepath, Arrays.asList("carpark.api_url=url", "carpark.accountKey=key"));
        // Try loading RDB configs
        try {
            loadAPIConfig.invoke(testConnector, filepath);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        // Retrieve private fields for apiKey, stationId and api_url. Check that they were set correctly
        Field accountKeyField = APIConnector.class.getDeclaredField("accountKey");
        accountKeyField.setAccessible(true);
        Assert.assertEquals("test_key", accountKeyField.get(testConnector));

        Field urlField = APIConnector.class.getDeclaredField("api_url");
        urlField.setAccessible(true);
        Assert.assertEquals("test_url", urlField.get(testConnector));
    }

    private void writePropertyFile(String filepath, List<String> properties) throws IOException {
        // Overwrite potentially existing properties file
        FileWriter writer = new FileWriter(filepath, false);
        // Populate file
        for (String s : properties) {
            writer.write(s + "\n");
        }
        // Close the file and return the file
        writer.close();
    }

    @Test
    public void testGetReadings() throws NoSuchFieldException, IllegalAccessException {

        // API returns a response
        JSONObject responseBody = new JSONObject();
        JSONObject asset = new JSONObject();
        double val=77.2;
        asset.put("testval", val);
        responseBody.put("testObject",asset);

        carparkAPIMock.stubFor(get(urlEqualTo("/v2/pws/observations/all/1day?accountKey=key"
                +"&format=json&units=s&numericPrecision=decimal"))
                .willReturn(ok().withBody(responseBody.toString())));
                //unsure about this portion

        Assert.assertEquals(responseBody.toString(), testConnector.getReadings().toString());

    }

}
