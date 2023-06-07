// test
package org.thingsboard.rule.engine.ftp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.rule.engine.api.util.TbNodeUtils;
import org.thingsboard.server.common.data.plugin.ComponentType;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

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
     private String file = "";

    @Override
    public void init(TbContext ctx, TbNodeConfiguration configuration) throws TbNodeException {
        config = TbNodeUtils.convert(configuration, TbFtpNodeConfiguration.class);

        if(testFtp()){
            scheduleUploadFtp();
        }
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) throws ExecutionException, InterruptedException, TbNodeException {
        String deviceName = msg.getMetaData().getData().get("deviceName");
        String tenantId = ctx.getTenantId().toString();
        if(msg.getType().equals("TB_MSG_TEST")){
            System.out.println("test message received");
            System.out.println("msg = " + msg.toString());
        }
        TbMsgMetaData metaData = msg.getMetaData();
        if(!metaData.getData().isEmpty()){
            if(msg.getType().equals("POST_TELEMETRY_REQUEST")){

                System.out.println("msg =" + msg.toString());
                this.file = "logs" + File.separator + tenantId + File.separator + deviceName;
                convertDataToCSV(msg, ctx, file);
            }
        }
    }

    @Override
    public void destroy() {
        file = null;
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
        LocalTime desiredTime = LocalTime.of(config.getScheduleHour(), config.getScheduleMinute()); // 12:08 PM

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
        Runnable scheduledTask = () -> {
            System.out.println("Scheduled task");
            try {
                uploadFtp();
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        };

        if (method.equals("DAILY")) {
            scheduledFuture = scheduler.scheduleAtFixedRate(scheduledTask, initialDelay, 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
        } else if (method.equals("WEEKLY")) {
            scheduledFuture = scheduler.scheduleAtFixedRate(scheduledTask, initialDelay, 7 * 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
        } else if (method.equals("MONTHLY")) {
            scheduledFuture = scheduler.scheduleAtFixedRate(scheduledTask, initialDelay, 30 * 24 * 60 * 60 * 1000, TimeUnit.MILLISECONDS);
        }

        System.out.println("Scheduled");
    }
    public void uploadFtp() throws FileNotFoundException {
        String folderPath = file.substring(0, file.lastIndexOf("\\"));
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

    public boolean testFtp (){
        FTPClient ftpClient = new FTPClient();
        boolean success = false;
        try {
            ftpClient.connect(config.getServerUrl(), config.getPort());
            success = ftpClient.login(config.getUsername(), config.getPassword());
            ftpClient.enterLocalPassiveMode();
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(ftpClient.isConnected()){
                    ftpClient.logout();
                    ftpClient.disconnect();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return success;
    }

    public static void convertDataToCSV(TbMsg msg, TbContext ctx, String csvFilePath) {

        List<String> timeseriesKeys = ctx.getTimeseriesService()
                .findAllKeysByEntityIds(ctx.getTenantId(), Collections.singletonList(msg.getOriginator()));

        Long timestamp = msg.getMetaDataTs();
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp);
        String deviceId = msg.getOriginator().getId().toString();
        String deviceName = msg.getMetaData().getData().get("deviceName");

        // Create the folder if it doesn't exist
        String folderPath = csvFilePath.substring(0, csvFilePath.lastIndexOf(File.separator));
        File folder = new File(folderPath);
        if (!folder.exists()) {
            boolean folderCreated = folder.mkdirs();
            if (!folderCreated) {
                // Handle folder creation failure
                System.err.println("Failed to create the folder: " + folderPath);
                return;
            }
        }

        try {
            File csvFile = new File(csvFilePath);
            boolean fileExists = csvFile.exists();

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(msg.getData());

            FileWriter writer = new FileWriter(csvFilePath, true);

            // Write headers to CSV file
            if (!fileExists) {
                writer.append("timestamp,date,deviceId,deviceName");

                for (String timeseriesKey : timeseriesKeys) {
                    // Check if the timeseriesKey exists as a key in the JSON data
                    writer.append(",").append(timeseriesKey);
                }
                writer.append("\n");
            }

            writer.append(timestamp.toString()).append(",").append(date).append(",")
                    .append(deviceId)
                    .append(",").append(deviceName);

            for (String timeseriesKey : timeseriesKeys) {
                // Check if the timeseriesKey exists as a key in the JSON data
                if (jsonNode.has(timeseriesKey)) {
                    writer.append(",").append(jsonNode.get(timeseriesKey).toString());
                } else {
                    writer.append(",");
                }
            }
            writer.append("\n");

            writer.flush();
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
