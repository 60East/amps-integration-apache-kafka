///////////////////////////////////////////////////////////////////////////
//
// Copyright (c) 2024-2026 60East Technologies Inc., All Rights Reserved.
//
// This computer software is owned by 60East Technologies Inc. and is
// protected by U.S. copyright laws and other laws and by international
// treaties.  This computer software is furnished by 60East Technologies
// Inc. pursuant to a written license agreement and may be used, copied,
// transmitted, and stored only in accordance with the terms of such
// license agreement and with the inclusion of the above copyright notice.
// This computer software or any other copies thereof may not be provided
// or otherwise made available to any other person.
//
// U.S. Government Restricted Rights.  This computer software: (a) was
// developed at private expense and is in all respects the proprietary
// information of 60East Technologies Inc.; (b) was not developed with
// government funds; (c) is a trade secret of 60East Technologies Inc.
// for all purposes of the Freedom of Information Act; and (d) is a
// commercial item and thus, pursuant to Section 12.212 of the Federal
// Acquisition Regulations (FAR) and DFAR Supplement Section 227.7202,
// Government's use, duplication or disclosure of the computer software
// is subject to the restrictions set forth by 60East Technologies Inc..
//
////////////////////////////////////////////////////////////////////////////

package com.crankuptheamps.kafka.testutils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaConnectClient {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String baseUrl;

    public KafkaConnectClient(String baseUrl) {
        this.baseUrl = trimTrailingSlash(baseUrl);
    }

    public void assertRunning() throws IOException {
        Response response = request("GET", "/connectors", null);
        if (response.status != HttpURLConnection.HTTP_OK) {
            throw new IOException("Kafka Connect is not available at " + baseUrl
                    + ": HTTP " + response.status + " " + response.body);
        }
    }

    public void createConnector(String name, Map<String, String> config) throws IOException {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("name", name);
        requestBody.put("config", config);

        Response response = request("POST", "/connectors", MAPPER.writeValueAsString(requestBody));
        if (response.status != HttpURLConnection.HTTP_CREATED
                && response.status != HttpURLConnection.HTTP_OK) {
            throw new IOException("Failed to create Kafka Connect connector " + name
                    + ": HTTP " + response.status + " " + response.body);
        }
    }

    public void deleteConnector(String name) throws IOException {
        Response response = request("DELETE", "/connectors/" + encode(name), null);
        if (response.status != HttpURLConnection.HTTP_OK
                && response.status != HttpURLConnection.HTTP_ACCEPTED
                && response.status != HttpURLConnection.HTTP_NO_CONTENT
                && response.status != HttpURLConnection.HTTP_NOT_FOUND) {
            throw new IOException("Failed to delete Kafka Connect connector " + name
                    + ": HTTP " + response.status + " " + response.body);
        }
    }

    public void waitForRunning(String name, int expectedTasks, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        String lastStatus = "";

        while (System.currentTimeMillis() < deadline) {
            Response response = request("GET", "/connectors/" + encode(name) + "/status", null);
            lastStatus = response.body;

            if (response.status == HttpURLConnection.HTTP_OK && isRunning(response.body, expectedTasks)) {
                return;
            }

            if (response.status == HttpURLConnection.HTTP_OK && response.body.contains("\"FAILED\"")) {
                throw new IllegalStateException("Kafka Connect connector " + name + " failed: " + response.body);
            }

            Thread.sleep(500L);
        }

        throw new IllegalStateException("Timed out waiting for Kafka Connect connector " + name
                + " to run. Last status: " + lastStatus);
    }

    @SuppressWarnings("unchecked")
    private static boolean isRunning(String json, int expectedTasks) throws IOException {
        Map<String, Object> status = MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
        Map<String, Object> connector = (Map<String, Object>) status.get("connector");
        List<Map<String, Object>> tasks = (List<Map<String, Object>>) status.get("tasks");

        if (connector == null || !"RUNNING".equals(connector.get("state"))) {
            return false;
        }

        if (tasks == null || tasks.size() != expectedTasks) {
            return false;
        }

        for (Map<String, Object> task : tasks) {
            if (!"RUNNING".equals(task.get("state"))) {
                return false;
            }
        }

        return true;
    }

    private Response request(String method, String path, String body) throws IOException {
        URL url = new URL(baseUrl + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(method);
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(10000);

        if (body != null) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", String.valueOf(bytes.length));
            try (OutputStream output = connection.getOutputStream()) {
                output.write(bytes);
            }
        }

        int status = connection.getResponseCode();
        InputStream stream = status >= 400 ? connection.getErrorStream() : connection.getInputStream();
        String responseBody = stream == null ? "" : readAll(stream);
        connection.disconnect();

        return new Response(status, responseBody);
    }

    private static String readAll(InputStream stream) throws IOException {
        try (InputStream input = stream; ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = input.read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        }
    }

    private static String encode(String value) throws IOException {
        return URLEncoder.encode(value, "UTF-8");
    }

    private static String trimTrailingSlash(String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static class Response {
        private final int status;
        private final String body;

        private Response(int status, String body) {
            this.status = status;
            this.body = body;
        }
    }
}
