package org.thingsboard.rule.engine.ftp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import org.thingsboard.rule.engine.api.NodeConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class TbFtpNodeConfiguration implements NodeConfiguration<TbFtpNodeConfiguration> {

    private String serverUrl;
    private String folder;
    private String username;
    private String password;
    private Integer port;
    private Integer tries;
    private Integer scheduleMinute;
    private Integer scheduleHour;
    private String scheduleMethod;



    @Override
    public TbFtpNodeConfiguration defaultConfiguration() {
        TbFtpNodeConfiguration configuration = new TbFtpNodeConfiguration();
        configuration.setServerUrl("127.0.0.1");
        configuration.setFolder("/ftp/");
        configuration.setUsername("ftpuser");
        configuration.setPassword("ftpuser");
        configuration.setPort(21);
        configuration.setTries(3);
        configuration.setScheduleHour(17);
        configuration.setScheduleMinute(31);
        configuration.setScheduleMethod("DAILY");
        return configuration;
    }
}
