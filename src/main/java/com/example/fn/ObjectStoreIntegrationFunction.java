package com.example.fn;

import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ObjectStoreIntegrationFunction {

    private static final String OBJECT_NAME = "TestFile.txt";
    private ObjectStorage objStoreClient = null;

    final ResourcePrincipalAuthenticationDetailsProvider provider
            = ResourcePrincipalAuthenticationDetailsProvider.builder().build();

    public ObjectStoreIntegrationFunction() {
        try {

            //print env vars in Functions container
            System.err.println("OCI_RESOURCE_PRINCIPAL_VERSION " + System.getenv("OCI_RESOURCE_PRINCIPAL_VERSION"));
            System.err.println("OCI_RESOURCE_PRINCIPAL_REGION " + System.getenv("OCI_RESOURCE_PRINCIPAL_REGION"));
            System.err.println("OCI_RESOURCE_PRINCIPAL_RPST " + System.getenv("OCI_RESOURCE_PRINCIPAL_RPST"));
            System.err.println("OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM " + System.getenv("OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM"));

            objStoreClient = new ObjectStorageClient(provider);

        } catch (Throwable ex) {
            System.err.println("Failed to instantiate ObjectStorage client - " + ex.getMessage());
        }
    }

    public String handle(String content) {

        if (objStoreClient == null) {
            String err = "There was a problem creating the ObjectStorage Client object. Please check logs";
            System.err.println(err);
            return err;
        }

        String data = "";
        try {
            String nameSpace = System.getenv().get("NAMESPACE");
            String bucketName = "bucket-demo";


            // Write File to Object Storage
            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .namespaceName(nameSpace)
                            .bucketName(bucketName)
                            .objectName(OBJECT_NAME)
                            .contentLength((long) content.length())
                            .putObjectBody(
                                    new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
                            .build();
            objStoreClient.putObject(putObjectRequest);



            // Get File from Object Storage
            GetObjectRequest getObjectRequest =
                    GetObjectRequest.builder()
                            .namespaceName(nameSpace)
                            .bucketName(bucketName)
                            .objectName(OBJECT_NAME)
                            .build();
            GetObjectResponse getObjectResponse = objStoreClient.getObject(getObjectRequest);


            // Read Content from the File
            StringBuilder sb = new StringBuilder();
            InputStream in = getObjectResponse.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line + System.lineSeparator());
            }
            data = sb.toString();

        } catch (Throwable e) {
            System.err.println("Error fetching object list from bucket " + e.getMessage());
        }

        return data;
    }
}