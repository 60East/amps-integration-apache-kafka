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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class ExampleTestUtils {
    public static final Path PROJECT_ROOT = Paths.get("").toAbsolutePath().normalize();
    public static final Path TEST_RESOURCES = PROJECT_ROOT.resolve("src/test/resources");
    public static final Path EXAMPLES_RESOURCES = TEST_RESOURCES.resolve("examples");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ExampleTestUtils() {
    }

    public static ConnectorExample loadExample(String relativePath) throws IOException {
        return MAPPER.readValue(EXAMPLES_RESOURCES.resolve(relativePath).toFile(), ConnectorExample.class);
    }

    public static List<Path> connectorExampleJsonFiles() throws IOException {
        try (Stream<Path> paths = Files.walk(EXAMPLES_RESOURCES)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .filter(path -> !"messages.json".equals(path.getFileName().toString()))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    public static Properties loadProperties(String relativePath) throws IOException {
        Properties properties = new Properties();
        try (InputStream stream = Files.newInputStream(EXAMPLES_RESOURCES.resolve(relativePath))) {
            properties.load(stream);
        }
        return properties;
    }

    public static List<Map<String, Object>> loadJsonLines(String relativePath) throws IOException {
        List<Map<String, Object>> messages = new ArrayList<>();
        Path path = EXAMPLES_RESOURCES.resolve(relativePath);

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    messages.add(MAPPER.readValue(trimmed, new TypeReference<Map<String, Object>>() { }));
                }
            }
        }

        return messages;
    }

    public static Map<String, String> config(String relativePath) throws IOException {
        return toStringMap(loadExample(relativePath).config);
    }

    public static Map<String, String> toStringMap(Map<String, ?> values) {
        Map<String, String> strings = new HashMap<>();
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            Object value = entry.getValue();
            strings.put(entry.getKey(), value == null ? null : String.valueOf(value));
        }
        return strings;
    }

    public static String fixedLengthJsonMessage(int targetLength) {
        String prefix = "{ \"message\" : \"";
        String suffix = "\" }";
        int used = prefix.length() + suffix.length();
        int fillLength = targetLength - used;

        if (fillLength < 0) {
            throw new IllegalArgumentException("Target length is too small for the JSON message wrapper");
        }

        return prefix + repeat('A', fillLength) + suffix;
    }

    public static String fixedLengthJsonMessage(String run, int sequence, int targetLength) {
        String prefix = "{\"run\":\"" + run + "\",\"seq\":" + sequence + ",\"message\":\"";
        String suffix = "\"}";
        int used = prefix.length() + suffix.length();
        int fillLength = targetLength - used;

        if (fillLength < 0) {
            throw new IllegalArgumentException("Target length is too small for the JSON message wrapper");
        }

        return prefix + repeat('A', fillLength) + suffix;
    }

    public static String jsonMessage(String run, int sequence) {
        return "{\"run\":\"" + run + "\",\"seq\":" + sequence + ",\"message\":\"" + sequence + "\"}";
    }

    private static String repeat(char value, int length) {
        char[] repeated = new char[length];
        Arrays.fill(repeated, value);
        return new String(repeated);
    }

    public static List<String> loadOrderMessagesWithRun(String relativePath, String run) throws IOException {
        List<String> messages = new ArrayList<>();
        Path path = EXAMPLES_RESOURCES.resolve(relativePath);

        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            int sequence = 0;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    Map<String, Object> message = MAPPER.readValue(trimmed,
                            new TypeReference<LinkedHashMap<String, Object>>() { });
                    message.put("run", run);
                    message.put("seq", sequence++);
                    messages.add(MAPPER.writeValueAsString(message));
                }
            }
        }

        return messages;
    }

    public static Map<String, Object> parseJson(String json) throws IOException {
        return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() { });
    }

    public static String writeJson(Object value) throws IOException {
        return MAPPER.writeValueAsString(value);
    }

    public static class ConnectorExample {
        public String name;
        public Map<String, Object> config;
    }
}
