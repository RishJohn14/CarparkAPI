package test.java.uk.ac.cam.cares.jps.agent.Carpark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.github.stefanbirkner.systemlambda.Statement;
import com.github.stefanbirkner.systemlambda.SystemLambda;
import uk.ac.cam.cares.jps.base.exception.JPSRuntimeException;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;

public class APIAgentLauncherTest {

    private static final Logger LOGGER = LogManager.getLogger(APIAgentLauncherTest.class);

    // Temporary folder to place a properties file
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    // Name of the properties files
    private final String agentPropertiesFilename = "agent.properties";
    private final String clientPropertiesFilename = "client.properties";
    private final String apiPropertiesFilename = "api.properties";
    // Argument array used with the main function containing all the paths to the property files as string
    private String[] args;

    @Before
    public void initializePropertyFile() throws IOException 
    {
        File agentPropertyFile= folder.newFile(agentPropertiesFilename);
        File clientPropertyFile= folder.newFile(clientPropertiesFilename);
        File apiPropertyFile= folder.newFile(apiPropertiesFilename);
        // Paths to the three different property files

        String agentPropertiesFile = agentPropertyFile.getCanonicalPath();
        String clientPropertiesFile = clientPropertyFile.getCanonicalPath();
        String apiPropertiesFile = apiPropertyFile.getCanonicalPath();
        args = new String[] {agentPropertiesFile, clientPropertiesFile, apiPropertiesFile};

    }

    @Test
    public void testProcessRequestParams() throws IOException 
    {
        APIAgentLauncher testLauncher = new APIAgentLauncher();
        //test empty requestparams
        JSONObject testRequestParams = new JSONObject();
        JSONObject testMessage = testLauncher.processRequestParameters(testRequestParams);
        Assert.assertEquals(testMessage.get("Result"), "Request parameters are not defined correctly.");

        //test non-empty requestParams but with incorrect keys
        testRequestParams.put("ageProperties", "TEST_AGENTPROPERTIES");
        testRequestParams.put("apiProperties", "TEST_APIPROPERTIES");
        testRequestParams.put("clientProperties", "TEST_CLIENTPROPERTIES");

        testMessage = testLauncher.processRequestParameters(testRequestParams);
        Assert.assertEquals(testMessage.get("Result"), "Request parameters are not defined correctly.");

        //test invalid environment variables in requestParams
        testRequestParams.remove("ageProperties");
        testRequestParams.put("agentProperties", "TEST_AGENTPROPERTIES");
        testRequestParams.put("apiProperties", "TEST_APIPROPERTIES");
        testRequestParams.put("clientProperties", "TEST_CLIENTPROPERTIES");

        String folderName = "mappings";
        File mappingFolder = folder.newFolder(folderName);
        // Create empty file in mappings folder
        File mappingFile = new File(Paths.get(mappingFolder.getCanonicalPath(), "carpark.properties").toString());
        Assert.assertTrue(mappingFile.createNewFile());
        //try and catch is required to use SystemLambda to mock environment variables
        //invalid environment variables TEST_AGENTPROPERTIE should cause validateInput to return back false and processRequestParameters to
        //return back the jsonMessage {"Result":"Request parameters are not defined correctly."}
        try {
            SystemLambda.withEnvironmentVariable("TEST_AGENTPROPERTIES", mappingFolder.getCanonicalPath()).execute((Statement) () -> {
                JSONObject testMessage01 = testLauncher.processRequestParameters(testRequestParams);
                Assert.assertEquals(testMessage01.get("Result"), "Request parameters are not defined correctly.");
            });
        } catch (Exception e) {
            //no Exception should be thrown here
        }
    }

    @Test
    public void testMainNoArgs() {
        String[] args = {};
        try {
            APIAgentLauncher.initializeAgent(args);
            Assert.fail();
        }
        catch (JPSRuntimeException e) {
            Assert.assertEquals("Need three properties files in the following order: 1) input agent 2) time series client 3) API connector.",
                    e.getMessage());
        }
    }

    @Test
    public void testMainInvalidAgentPropertyFile() {
        // Empty agent properties file should result in an error
        try {
            APIAgentLauncher.initializeAgent(args);
            Assert.fail();
        }
        catch (JPSRuntimeException e) {
            Assert.assertEquals("The Carpark input agent could not be constructed!", e.getMessage());
        }
    }

    @Test
    public void testMainErrorWhenCreatingTSClient() throws IOException {
        //create agent properties file
        createProperAgentPropertiesFile();
        //File testFile=new File(Paths.get(args[0],"agent.properties").toString());
        //Assert.assertTrue(testFile.exists());
        //Create folder with mapping file
        String folderName = "mappings";
        File mappingFolder = folder.newFolder(folderName);
        // Create empty file in mappings folder
        File mappingFile = new File(Paths.get(mappingFolder.getCanonicalPath(), "carpark.properties").toString());
        Assert.assertTrue(mappingFile.createNewFile());
        // Empty properties file for time series client should result in exception
        try {
            SystemLambda.withEnvironmentVariable("TEST_MAPPINGS", mappingFolder.getCanonicalPath()).execute(() -> {
                APIAgentLauncher.initializeAgent(args);
            });
        }
        catch (Exception e) {
            Assert.assertEquals("Could not construct the time series client needed by the input agent!", e.getMessage());
        }

    }

    @Test
    public void testMainErrorWhenCreatingAPIConnector() throws IOException {
        createProperClientPropertiesFile();
        // Use a mock for the input agent
        try(MockedConstruction<APIInputAgent> mockAgent = Mockito.mockConstruction(APIInputAgent.class)) {
            // Empty API properties file should result in an exception
            try {
                APIAgentLauncher.initializeAgent(args);
                Assert.fail();
            }
            catch (JPSRuntimeException e) {
                // Ensure that the method to set the time series client was invoked once
                Mockito.verify(mockAgent.constructed().get(0), Mockito.times(1)).setTsClient(Mockito.any());
                // Ensure that the initialization was invoked once
                Mockito.verify(mockAgent.constructed().get(0), Mockito.times(1)).initializeTimeSeriesIfNotExist();
                Assert.assertEquals("Could not construct the carpark API connector needed to interact with the API!", e.getMessage());
            }
        }

    }

    @Test
    public void testMainErrorWhenRetrievingReadings() throws IOException {
        createProperClientPropertiesFile();
        createProperAPIPropertiesFile();
        // Use a mock for the input agent
        try(MockedConstruction<APIInputAgent> ignored = Mockito.mockConstruction(APIInputAgent.class)) {
            // Use a mock for the connector that throws an exception when readings are requested
            try(MockedConstruction<APIConnector> mockConnector = Mockito.mockConstruction(APIConnector.class,
                    (mock, context) -> Mockito.when(mock.getReadings()).thenThrow(new JPSRuntimeException("exception")))) {
                try {
                    APIAgentLauncher.initializeAgent(args);
                    Assert.fail();
                }
                catch (JPSRuntimeException e) {
                    Assert.assertEquals("Some readings could not be retrieved.", e.getMessage());
                    Assert.assertEquals(JPSRuntimeException.class, e.getCause().getClass());
                    Assert.assertEquals("exception", e.getCause().getMessage());
                }
            }
        }
    }


    @Test
    public void testReadingsNotEmpty() throws IOException {
        createProperClientPropertiesFile();
        createProperAPIPropertiesFile();
        // Create dummy readings to return
       
        String[] keys = {"CarParkID","Area","Development","Location","AvailableLots","LotType","Agency"};
        String[] carparks = {"Carpark1","Carpark2","Carpark3"};
        JSONObject readings = newJSONObject();
        JSONArray jsArr = new JSONArray();
        String value="";

        for(int i=0;i<3;i++)
        {
            JSONObject currentCarpark = new JSONObject();
            currentCarpark.put("CarParkID",carparks[i]);

            for(int j=1;j<7;j++)
            {
                currentCarpark.put(keys[j],value);
            }
            jsArr.put(i,currentCarpark);
        }

        readings.put("value",jsArr);


        // Use a mock for the input agent
        try(MockedConstruction<APIInputAgent> mockAgent = Mockito.mockConstruction(APIInputAgent.class)) {
            // Use a mock for the connector that returns the dummy readings
            try(MockedConstruction<APIConnector> ignored = Mockito.mockConstruction(APIConnector.class,
                    (mock, context) -> {
                        Mockito.when(mock.getReadings()).thenReturn(readings);
                    })) {
                APIAgentLauncher.initializeAgent(args);
                // Ensure that the update of the agent was invoked
                Mockito.verify(mockAgent.constructed().get(0), Mockito.times(1)).updateData(readings);
            }
        }
    }

    private void createProperAgentPropertiesFile() throws IOException {
        // Create a properties file that points to the example/test mapping folder in the resources //
        // Create mappings folder
        // Filepath for the properties file
        String propertiesFile = Paths.get(folder.getRoot().toString(), agentPropertiesFilename).toString();
        try (FileWriter writer = new FileWriter(propertiesFile, false)) {
            writer.write("carpark.mappingfolder=TEST_MAPPINGS");
        }
    }

    private void createProperClientPropertiesFile() throws IOException {
        // Filepath for the properties file
        String propertiesFile = Paths.get(folder.getRoot().toString(), clientPropertiesFilename).toString();
        try (FileWriter writer = new FileWriter(propertiesFile, false)) {
            writer.write("db.url=jdbc:postgresql://host.docker.internal:5432/postgres\n");
            writer.write("db.user=postgres\n");
            writer.write("db.password=postgres\n");
            writer.write("sparql.query.endpoint=http://host.docker.internal:9999/blazegraph/namespace/test/sparql\n");
            writer.write("sparql.update.endpoint=http://host.docker.internal:9999/blazegraph/namespace/test/sparql\n");
        }
    }

    private void createProperAPIPropertiesFile() throws IOException {
        // Filepath for the properties file
        String propertiesFile = Paths.get(folder.getRoot().toString(), apiPropertiesFilename).toString();
        try (FileWriter writer = new FileWriter(propertiesFile, false)) {
            writer.write("carpark.accountKey=16bfghijk8910111213145nni99b897r\n");
            writer.write("carpark.api_url=http://localhost:8080/");

        }
    }
}
