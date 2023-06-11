// test
package org.thingsboard.rule.engine.ftp;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.Device;
import org.thingsboard.server.common.data.id.DeviceId;
import org.thingsboard.server.common.data.kv.Aggregation;
import org.thingsboard.server.common.data.kv.BaseReadTsKvQuery;
import org.thingsboard.server.common.data.kv.ReadTsKvQuery;
import org.thingsboard.server.common.data.kv.TsKvEntry;
import org.thingsboard.server.common.data.page.PageLink;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
@RuleNode(
        type = ComponentType.EXTERNAL,
        name = "Ftp",
        configClazz = TbFtpNodeConfiguration.class,
        nodeDescription = "Invoke FTP calls to external REST server",
        nodeDetails = "Tested with Apache FTP server",
        uiResources = {"static/rulenode/custom-nodes-config.js"},
        configDirective = "tbExternalNodeFtpConfig",
        iconUrl = "data:image/svg+xml;base64,PHN2ZyBzdHlsZT0iZW5hYmxlLWJhY2tncm91bmQ6bmV3IDAgMCA1MTIgNTEyIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbDpzcGFjZT0icHJlc2VydmUiIHZpZXdCb3g9IjAgMCA1MTIgNTEyIiB2ZXJzaW9uPSIxLjEiIHk9IjBweCIgeD0iMHB4Ij48ZyB0cmFuc2Zvcm09Im1hdHJpeCguOTQ5NzUgMCAwIC45NDk3NSAxNy4xMiAyNi40OTIpIj48cGF0aCBkPSJtMTY5LjExIDEwOC41NGMtOS45MDY2IDAuMDczNC0xOS4wMTQgNi41NzI0LTIyLjAxNCAxNi40NjlsLTY5Ljk5MyAyMzEuMDhjLTMuNjkwNCAxMi4xODEgMy4yODkyIDI1LjIyIDE1LjQ2OSAyOC45MSAyLjIyNTkgMC42NzQ4MSA0LjQ5NjkgMSA2LjcyODUgMSA5Ljk3MjEgMCAxOS4xNjUtNi41MTUzIDIyLjE4Mi0xNi40NjdhNi41MjI0IDYuNTIyNCAwIDAgMCAwLjAwMiAtMC4wMDJsNjkuOTktMjMxLjA3YTYuNTIyNCA2LjUyMjQgMCAwIDAgMCAtMC4wMDJjMy42ODU1LTEyLjE4MS0zLjI4Ny0yNS4yMjUtMTUuNDcxLTI4LjkxMi0yLjI4MjUtMC42OTE0NS00LjYxMTYtMS4wMTY5LTYuODk4NC0xem04NC45ODggMGMtOS45MDQ4IDAuMDczNC0xOS4wMTggNi41Njc1LTIyLjAxOCAxNi40NjlsLTY5Ljk4NiAyMzEuMDhjLTMuNjg5OCAxMi4xNzkgMy4yODUzIDI1LjIxNyAxNS40NjUgMjguOTA4IDIuMjI5NyAwLjY3NjQ3IDQuNTAwOCAxLjAwMiA2LjczMjQgMS4wMDIgOS45NzIxIDAgMTkuMTY1LTYuNTE1MyAyMi4xODItMTYuNDY3YTYuNTIyNCA2LjUyMjQgMCAwIDAgMC4wMDIgLTAuMDAybDY5Ljk4OC0yMzEuMDdjMy42OTA4LTEyLjE4MS0zLjI4NTItMjUuMjIzLTE1LjQ2Ny0yOC45MTItMi4yODE0LTAuNjkyMzEtNC42MTA4LTEuMDE4OS02Ljg5ODQtMS4wMDJ6bS0yMTcuMjkgNDIuMjNjLTEyLjcyOS0wLjAwMDg3LTIzLjE4OCAxMC40NTYtMjMuMTg4IDIzLjE4NiAwLjAwMSAxMi43MjggMTAuNDU5IDIzLjE4NiAyMy4xODggMjMuMTg2IDEyLjcyNy0wLjAwMSAyMy4xODMtMTAuNDU5IDIzLjE4NC0yMy4xODYgMC4wMDA4NzYtMTIuNzI4LTEwLjQ1Ni0yMy4xODUtMjMuMTg0LTIzLjE4NnptMCAxNDYuNjRjLTEyLjcyNy0wLjAwMDg3LTIzLjE4NiAxMC40NTUtMjMuMTg4IDIzLjE4NC0wLjAwMDg3MyAxMi43MjkgMTAuNDU4IDIzLjE4OCAyMy4xODggMjMuMTg4IDEyLjcyOC0wLjAwMSAyMy4xODQtMTAuNDYgMjMuMTg0LTIzLjE4OC0wLjAwMS0xMi43MjYtMTAuNDU3LTIzLjE4My0yMy4xODQtMjMuMTg0em0yNzAuNzkgNDIuMjExYy0xMi43MjcgMC0yMy4xODQgMTAuNDU3LTIzLjE4NCAyMy4xODRzMTAuNDU1IDIzLjE4OCAyMy4xODQgMjMuMTg4aDE1NC45OGMxMi43MjkgMCAyMy4xODYtMTAuNDYgMjMuMTg2LTIzLjE4OCAwLjAwMS0xMi43MjgtMTAuNDU4LTIzLjE4NC0yMy4xODYtMjMuMTg0eiIgdHJhbnNmb3JtPSJtYXRyaXgoMS4wMzc2IDAgMCAxLjAzNzYgLTcuNTY3NiAtMTQuOTI1KSIgc3Ryb2tlLXdpZHRoPSIxLjI2OTMiLz48L2c+PC9zdmc+"
)
public class TbFtpNode implements TbNode {
    private TbFtpNodeConfiguration config;
    private ScheduledFuture<?> scheduledFuture;
    private String folderPath = "D:\\logs";
    private List<Device> deviceList;
    private TbContext ctx;


    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, TbFtpNodeConfiguration.class);
        this.ctx = ctx;
        deviceList = new ArrayList<>();
        System.out.println("initializing FTP node");
//        scheduleUploadFtp();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {


        if (msg.getType().equals("TB_MSG_TEST")) {

            System.out.println("onMsg FTP node TB_MSG_TEST");
            System.out.println("msg = " + msg);
            String url = msg.getMetaData().getData().get("url");
            System.out.println("url = " + url);

            String username = msg.getMetaData().getData().get("username");
            System.out.println("username = " + username);

            String password = msg.getMetaData().getData().get("password");
            System.out.println("password = " + password);

            Integer port = Integer.valueOf(msg.getMetaData().getData().get("port"));
            System.out.println("port = " + port);

            String scheduleMethod = msg.getMetaData().getData().get("scheduleMethod");
            System.out.println("scheduleMethod = " + scheduleMethod);

            Integer scheduleHour = Integer.valueOf(msg.getMetaData().getData().get("scheduleHour"));
            System.out.println("scheduleHour = " + scheduleHour);

            Integer scheduleMinute = Integer.valueOf(msg.getMetaData().getData().get("scheduleMinute"));
            System.out.println("scheduleMinute = " + scheduleMinute);



            if(!(config.getServerUrl().equals(url)
                    && config.getUsername().equals(username)
                    && config.getPassword().equals(password)
                    && config.getPort().equals(port)
                    && config.getScheduleMethod().equals(scheduleMethod)
                    && config.getScheduleHour().equals(scheduleHour)
                    && config.getScheduleMinute().equals(scheduleMinute))){
                System.out.println("Configuring FTP node");

                config.setServerUrl(url);
                config.setUsername(username);
                config.setPassword(password);
                config.setPort(port);
                config.setScheduleMethod(scheduleMethod);
                config.setScheduleHour(scheduleHour);
                config.setScheduleMinute(scheduleMinute);

                if(testFtpConnection(url, username, password, port)){
                    System.out.println("Ftp +++");

                    String deviceId = msg.getMetaData().getData().get("deviceId");
                    System.out.println("deviceId = " + deviceId);
                    if (deviceId == null || deviceId.isEmpty()) {
                        System.out.println("deviceId is empty");
                        this.deviceList.clear();
                        this.deviceList.addAll(ctx.getDeviceService().findDevicesByTenantIdAndCustomerId(ctx.getTenantId(), msg.getCustomerId(), new PageLink(100))
                                .getData());

                    }else {
                        System.out.println("deviceId is not empty");
                        this.deviceList.clear();
                        this.deviceList.add(ctx.getDeviceService().findDeviceByIdAsync(ctx.getTenantId(), new DeviceId(UUID.fromString(deviceId))).get());
                    }
                    System.out.println("deviceList = " + deviceList);
                    scheduleUploadFtp();
                }
            }

        }

        TbMsgMetaData metaData = msg.getMetaData();
        if (!metaData.getData().isEmpty()) {
            if (msg.getType().equals("POST_TELEMETRY_REQUEST")) {

//                System.out.println("msg =" + msg.toString());
//                this.folderPath = "D:\\logs\\file.csv";
//                convertDataToCSV(msg, ctx, folderPath);
            }
        }
    }

    @Override
    public void destroy() {
        folderPath = null;
        config = null;
        scheduledFuture.cancel(true);
        System.out.println("Destroying");
    }

    private void scheduleUploadFtp() {
        System.out.println("Scheduling");
        // Get the current date and time in the default time zone
        ZonedDateTime now = ZonedDateTime.now(ZoneId.systemDefault());

        // Get frequency method
        String method = config.getScheduleMethod();

        // Set the desired time for the event
        LocalTime desiredTime = LocalTime.of(config.getScheduleHour(), config.getScheduleMinute());

        // Combine the current date and desired time
        ZonedDateTime scheduledTime = now.with(desiredTime);

        // If the scheduled time is already in the past, move it to the next day/week/month
        if (now.compareTo(scheduledTime) > 0) {
            if (method.equals("DAILY")) {
                scheduledTime = scheduledTime.plusDays(1);
            } else if (method.equals("WEEKLY")) {
                scheduledTime = scheduledTime.plusWeeks(1);
            } else if (method.equals("MONTHLY")) {
                scheduledTime = scheduledTime.plusMonths(1);
            }
        }

        // Calculate the initial delay until the scheduled time
        long initialDelay = now.until(scheduledTime, ChronoUnit.MILLIS);

        // Create a scheduled executor service
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Schedule the task with the appropriate interval based on the selected method
        ZonedDateTime finalScheduledTime = scheduledTime;
        Runnable scheduledTask = () -> {

            System.out.println("Scheduled task");

            long nowTime = now.toInstant().toEpochMilli();
            long endTs = new Date().getTime();
            long startTs = nowTime + (endTs - finalScheduledTime.toInstant().toEpochMilli());

            System.out.println("nowTime = " + nowTime);
            System.out.println("startTs = " + startTs);
            System.out.println("endTs = " + endTs);

            deviceList.forEach(device -> {

                long interval = 0L;
                int limit = 200;
                Aggregation agg = Aggregation.NONE;
                String orderBy = "DESC";
                List<String> keys = ctx.getTimeseriesService().findAllKeysByEntityIds(ctx.getTenantId(), Collections.singletonList(device.getId()));

                List<ReadTsKvQuery> queries = keys.stream().map(key ->
                                new BaseReadTsKvQuery(key, startTs, endTs, interval, limit, agg, orderBy))
                        .collect(Collectors.toList());
                try {
                    List<TsKvEntry> result = ctx.getTimeseriesService()
                            .findAll(ctx.getTenantId(), device.getId(), queries).get();
                    convertDataToCSV(result, keys, device);
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            try {
                uploadFtp();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        };

        switch (method) {
            case "DAILY":
                scheduledFuture = scheduler.scheduleAtFixedRate(scheduledTask, initialDelay, 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
                break;
            case "WEEKLY":
                scheduledFuture = scheduler.scheduleAtFixedRate(scheduledTask, initialDelay, 7 * 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
                break;
            case "MONTHLY":
                scheduledFuture = scheduler.scheduleAtFixedRate(scheduledTask, initialDelay, 30 * 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
                break;
        }

            System.out.println("Scheduled");
        }

        public void uploadFtp () throws FileNotFoundException {
            File folder = new File(folderPath);

            if (folder.exists() && folder.isDirectory()) {
                File[] files = folder.listFiles();

                FTPClient ftpClient = new FTPClient();

                try {
                    ftpClient.connect(config.getServerUrl(), config.getPort());
                    ftpClient.login(config.getUsername(), config.getPassword());
                    ftpClient.enterLocalPassiveMode();
                    ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

                    for (File file : files) {
                        if (file.isFile()) {
                            String filename = file.getName();
                            InputStream inputStream = new FileInputStream(file);

                            try {
                                if (!ftpClient.changeWorkingDirectory(config.getFolder())) {
                                    ftpClient.makeDirectory(config.getFolder());
                                }
                                ftpClient.appendFile(filename, inputStream);
                            } catch (IOException e) {
                                e.printStackTrace();
                            } finally {
                                try {
                                    inputStream.close();
                                    file.delete();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        ftpClient.logout();
                        ftpClient.disconnect();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                throw new FileNotFoundException("Folder not found");
            }
        }

        public boolean testFtpConnection(String url, String username, String password, Integer port){
            FTPClient ftpClient = new FTPClient();
            boolean success = false;
            try {
                ftpClient.connect(url, port);
                success = ftpClient.login(username, password);
                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (ftpClient.isConnected()) {
                        ftpClient.logout();
                        ftpClient.disconnect();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return success;
        }

        public void convertDataToCSV (List < TsKvEntry > result, List < String > timeseriesKeys, Device device){

                // Create the folder if it doesn't exist
                File folder = new File(folderPath);
                if (!folder.exists()) {
                    boolean folderCreated = folder.mkdirs();
                    if (!folderCreated) {
                        System.err.println("Failed to create the folder: " + folderPath);
                        return;
                    }
                }

                try {
                    File csvFile = new File(folderPath + File.separator + device.getName() + ".csv");
                    System.out.println("csvFile: " + csvFile);
                    boolean fileExists = csvFile.exists();

                    FileWriter writer = new FileWriter(csvFile, true);

                    // Write headers to CSV file if it's a new file
                    if (!fileExists) {
                        writer.append("timestamp,date,deviceId,deviceName");
                        for (String timeseriesKey : timeseriesKeys) {
                            writer.append(",").append(timeseriesKey);
                        }
                        writer.append("\n");
                    }

                    Map<Long, Map<String, String>> dataMap = new HashMap<>();

                    for (TsKvEntry entry : result) {
                        Long timestamp = entry.getTs();
                        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp);

                        // Check if the timestamp already exists in the data map
                        Map<String, String> dataRow = dataMap.get(timestamp);
                        if (dataRow == null) {
                            dataRow = new HashMap<>();
                            dataRow.put("timestamp", timestamp.toString());
                            dataRow.put("date", date);
                            dataRow.put("deviceId", device.getId().toString());
                            dataRow.put("deviceName", device.getName());
                            dataMap.put(timestamp, dataRow);
                        }

                        // Check if the timeseriesKey exists as a key in the JSON data
                        if (timeseriesKeys.contains(entry.getKey())) {
                            dataRow.put(entry.getKey(), entry.getValueAsString());
                        }
                    }

                    // Write the data rows to the CSV file
                    for (Map<String, String> dataRow : dataMap.values()) {
                        writer.append(dataRow.get("timestamp")).append(",")
                                .append(dataRow.get("date")).append(",")
                                .append(dataRow.get("deviceId")).append(",")
                                .append(dataRow.get("deviceName"));

                        for (String timeseriesKey : timeseriesKeys) {
                            writer.append(",").append(dataRow.getOrDefault(timeseriesKey, ""));
                        }

                        writer.append("\n");
                    }

                    writer.flush();
                    writer.close();

                    System.out.println("CSV file created successfully.");

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
}
