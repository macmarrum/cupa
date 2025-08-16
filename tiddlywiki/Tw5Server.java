/*
 * Copyright (C) 2025  macmarrum (at) outlook (dot) ie
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

import com.sun.net.httpserver.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.*;

public class Tw5Server {
    private static final int PORT = 8000;
    private static final Path WORK_DIR = Paths.get(System.getProperty("user.dir"));
    private static final Path LOG_FILE = WORK_DIR.resolve("tw5-server-" + PORT + ".log");
    private static final Path BACKUP_DIR = WORK_DIR;
    private static final Path DOC_ROOT = WORK_DIR;
    private static final Path ALLOWED_CLIENT_ADDRESSES_FILE = WORK_DIR.resolve("allowed-client-addresses.txt");
    private static final Set<String> ALLOWED_CLIENT_ADDRESSES = loadAllowedClientAddresses(ALLOWED_CLIENT_ADDRESSES_FILE);

    public static void main(String[] args) throws IOException {
        var host = ALLOWED_CLIENT_ADDRESSES.equals(Set.of("127.0.0.1")) ? "127.0.0.1" : "0.0.0.0";
        var server = HttpServer.create(new InetSocketAddress(host, PORT), 0);
        server.createContext("/", new TiddlyWikiHandler());
        server.setExecutor(null);
        server.start();
        var dateTime = getCurrentDateTime();
        System.out.printf(":: %s - %s - %s%n", System.getProperty("java.vm.name"), System.getProperty("java.vm.version"), System.getProperty("java.vm.vendor"));
        System.out.printf(":: %s -- serving '%s' at %s %d%n", dateTime, WORK_DIR, host, PORT);
        System.out.printf(":: %s -- WORK_DIR: %s%n", dateTime, WORK_DIR);
        System.out.printf(":: %s -- allowed client addresses: %s%n", dateTime, ALLOWED_CLIENT_ADDRESSES);
    }

    private static Set<String> loadAllowedClientAddresses(Path allowedClientAddressesFile) {
        var result = new HashSet<String>();
        result.add("127.0.0.1");
        try {
            var addresses = Files.readAllLines(allowedClientAddressesFile, StandardCharsets.UTF_8);
            for (var address : addresses) {
                address = address.trim();
                if (!address.isEmpty() && !address.startsWith("#")) {
                    result.add(address);
                }
            }
        } catch (IOException e) {
            System.err.printf(":: %s: %s%n", e.getClass().getName(), e.getMessage());
        }
        return result;
    }

    private static String getCurrentDateTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd, EEE HH:mm:ss"));
    }

    static class TiddlyWikiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var remoteAddress = exchange.getRemoteAddress().getAddress().getHostAddress();
            if (!ALLOWED_CLIENT_ADDRESSES.contains(remoteAddress)) {
                logMessage(exchange, String.format("IP not in %s: %s", ALLOWED_CLIENT_ADDRESSES_FILE.getFileName(), ALLOWED_CLIENT_ADDRESSES));
                sendError(exchange, 403, "Forbidden");
                return;
            }
            var method = exchange.getRequestMethod();
            try {
                switch (method) {
                    case "GET" -> handleGet(exchange);
                    case "PUT" -> handlePut(exchange);
                    case "OPTIONS" -> handleOptions(exchange);
                    case "HEAD" -> handleHead(exchange);
                    default -> sendError(exchange, 501, "Not Implemented");
                }
            } catch (Exception e) {
                logMessage(exchange, "Error: " + e.getMessage());
                sendError(exchange, 500, "Internal Server Error");
            } finally {
                exchange.close();
            }
        }

        private void handleGet(HttpExchange exchange) throws IOException {
            var requestPath = exchange.getRequestURI().getPath();
            logMessage(exchange, "GET " + requestPath);
            var path = Paths.get(requestPath).normalize();
            var filePath = DOC_ROOT.resolve(path.toString().substring(1));
            if (!filePath.startsWith(DOC_ROOT)) {
                sendError(exchange, 403, "Forbidden");
                return;
            }
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                var response = Files.readAllBytes(filePath);
                var mimeType = getMimeType(filePath.toString());
                exchange.getResponseHeaders().set("Content-Type", mimeType);
                exchange.sendResponseHeaders(200, response.length);
                try (var os = exchange.getResponseBody()) {
                    os.write(response);
                }
                logMessage(exchange, String.format("%s %s - %d", exchange.getRequestMethod(), requestPath, 200));
            } else {
                sendError(exchange, 404, "Not Found");
            }
        }

        private void handlePut(HttpExchange exchange) throws IOException {
            var requestPath = exchange.getRequestURI().getPath();
            logMessage(exchange, "PUT " + requestPath);
            var path = Paths.get(requestPath).normalize();
            var filePath = DOC_ROOT.resolve(path.toString().substring(1));
            if (!filePath.startsWith(DOC_ROOT)) {
                sendError(exchange, 403, "Forbidden");
                return;
            }
            Files.createDirectories(filePath.getParent());
            var data = exchange.getRequestBody().readAllBytes();
            Files.write(filePath, data);
            addDataToZipFile(filePath, data);
            exchange.sendResponseHeaders(200, -1);
            logMessage(exchange, String.format("%s %s - %d", exchange.getRequestMethod(), requestPath, 200));
        }

        private void handleOptions(HttpExchange exchange) throws IOException {
            var requestPath = exchange.getRequestURI().getPath();
            logMessage(exchange, "OPTIONS " + requestPath);
            exchange.getResponseHeaders().add("Allow", "GET,HEAD,OPTIONS,PUT");
            exchange.getResponseHeaders().add("x-api-access-type", "file");
            exchange.getResponseHeaders().add("dav", "tw5/put");
            exchange.sendResponseHeaders(200, -1);
            logMessage(exchange, String.format("%s %s - %d", exchange.getRequestMethod(), requestPath, 200));
        }

        private void handleHead(HttpExchange exchange) throws IOException {
            var requestPath = exchange.getRequestURI().getPath();
            logMessage(exchange, "HEAD " + requestPath);
            var path = Paths.get(requestPath).normalize();
            var filePath = DOC_ROOT.resolve(path.toString().substring(1));
            if (Files.exists(filePath) && Files.isRegularFile(filePath)) {
                var mimeType = getMimeType(filePath.toString());
                exchange.getResponseHeaders().set("Content-Type", mimeType);
                exchange.sendResponseHeaders(200, -1);
                logMessage(exchange, String.format("%s %s - %d", exchange.getRequestMethod(), requestPath, 200));
            } else {
                sendError(exchange, 404, "Not Found");
            }
        }

        private String getMimeType(String path) {
            int lastDot = path.lastIndexOf('.');
            var ext = (lastDot > 0) ? path.substring(lastDot + 1).toLowerCase() : "";
            return switch (ext) {
                case "html", "htm" -> "text/html; charset=utf-8";
                case "ico" -> "image/vnd.microsoft.icon";
                case "css" -> "text/css; charset=utf-8";
                case "js" -> "application/javascript; charset=utf-8";
                case "json" -> "application/json; charset=utf-8";
                case "png" -> "image/png";
                case "jpg", "jpeg" -> "image/jpeg";
                default -> "application/octet-stream";
            };
        }

        private void addDataToZipFile(Path filePath, byte[] data) throws IOException {
            var fileName = filePath.getFileName().toString();
            int lastDotIndex = fileName.lastIndexOf('.');
            var fileStem = (lastDotIndex > 0) ? fileName.substring(0, lastDotIndex) : fileName;
            var zipPath = BACKUP_DIR.resolve(fileStem + "-tw5.zip");
            var timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH·mm·ss"));
            var entryName = timestamp + "~" + filePath.getFileName();
            try (var zos = new ZipOutputStream(new FileOutputStream(zipPath.toFile(), true))) {
                zos.setMethod(ZipOutputStream.DEFLATED);
                zos.setLevel(Deflater.BEST_COMPRESSION);
                var entry = new ZipEntry(entryName);
                zos.putNextEntry(entry);
                zos.write(data);
                zos.closeEntry();
            }
        }

        private void sendError(HttpExchange exchange, int code, String message)
                throws IOException {
            logMessage(exchange, String.format("%d %s - %s %s", code, message, exchange.getRequestMethod(), exchange.getRequestURI().getPath()));
            var response = message.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(code, response.length);
            try (var os = exchange.getResponseBody()) {
                os.write(response);
            }
        }

        private void logMessage(HttpExchange exchange, String message) {
            var dateTime = getCurrentDateTime();
            var logMessage = String.format("%s - - [%s] %s%n", exchange.getRemoteAddress().getAddress().getHostAddress(), dateTime, message);
            System.err.print(logMessage);
            try {
                Files.writeString(LOG_FILE, logMessage, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
