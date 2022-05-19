/**
 * Copyright © 2016-2022 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.dao.settings;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.thingsboard.server.common.data.AdminSettings;
import org.thingsboard.server.common.data.id.AdminSettingsId;
import org.thingsboard.server.common.data.id.TenantId;
import org.thingsboard.server.common.data.vc.VersionControlAuthMethod;
import org.thingsboard.server.dao.service.DataValidator;
import org.thingsboard.server.dao.service.Validator;

@Service
@Slf4j
public class AdminSettingsServiceImpl implements AdminSettingsService {
    
    @Autowired
    private AdminSettingsDao adminSettingsDao;

    @Autowired
    private DataValidator<AdminSettings> adminSettingsValidator;

    @Override
    public AdminSettings findAdminSettingsById(TenantId tenantId, AdminSettingsId adminSettingsId) {
        log.trace("Executing findAdminSettingsById [{}]", adminSettingsId);
        Validator.validateId(adminSettingsId, "Incorrect adminSettingsId " + adminSettingsId);
        return  adminSettingsDao.findById(tenantId, adminSettingsId.getId());
    }

    @Override
    public AdminSettings findAdminSettingsByKey(TenantId tenantId, String key) {
        log.trace("Executing findAdminSettingsByKey [{}]", key);
        Validator.validateString(key, "Incorrect key " + key);
        return adminSettingsDao.findByTenantIdAndKey(tenantId.getId(), key);
    }

    @Override
    public AdminSettings saveAdminSettings(TenantId tenantId, AdminSettings adminSettings) {
        log.trace("Executing saveAdminSettings [{}]", adminSettings);
        adminSettingsValidator.validate(adminSettings, data -> tenantId);
        if (adminSettings.getKey().equals("mail") && !adminSettings.getJsonValue().has("password")) {
            AdminSettings mailSettings = findAdminSettingsByKey(tenantId, "mail");
            if (mailSettings != null) {
                ((ObjectNode) adminSettings.getJsonValue()).put("password", mailSettings.getJsonValue().get("password").asText());
            }
        } else if (adminSettings.getKey().equals("entitiesVersionControl")) {
            VersionControlAuthMethod authMethod = VersionControlAuthMethod.valueOf(adminSettings.getJsonValue().get("authMethod").asText());
            if (VersionControlAuthMethod.USERNAME_PASSWORD.equals(authMethod) && !adminSettings.getJsonValue().has("password")) {
                AdminSettings vcSettings = findAdminSettingsByKey(tenantId, "entitiesVersionControl");
                if (vcSettings != null) {
                    ((ObjectNode) adminSettings.getJsonValue()).put("password", vcSettings.getJsonValue().get("password").asText());
                }
            } else if (VersionControlAuthMethod.PRIVATE_KEY.equals(authMethod) && !adminSettings.getJsonValue().has("privateKey")) {
                AdminSettings vcSettings = findAdminSettingsByKey(tenantId, "entitiesVersionControl");
                if (vcSettings != null) {
                    ((ObjectNode) adminSettings.getJsonValue()).put("privateKey", vcSettings.getJsonValue().get("privateKey").asText());
                    if (!adminSettings.getJsonValue().has("privateKeyPassword") && vcSettings.getJsonValue().has("privateKeyPassword")) {
                        ((ObjectNode) adminSettings.getJsonValue()).put("privateKeyPassword", vcSettings.getJsonValue().get("privateKeyPassword").asText());
                    }
                }
            }
        }
        return adminSettingsDao.save(tenantId, adminSettings);
    }

}
