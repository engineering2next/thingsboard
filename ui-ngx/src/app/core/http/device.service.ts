///
/// Copyright © 2016-2023 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { Injectable } from '@angular/core';
import { defaultHttpOptionsFromConfig, RequestConfig } from './http-utils';
import { Observable, ReplaySubject } from 'rxjs';
import { HttpClient, HttpRequest, HttpResponse } from '@angular/common/http';
import { PageLink } from '@shared/models/page/page-link';
import { PageData } from '@shared/models/page/page-data';
import {
  ClaimRequest,
  ClaimResult,
  Device,
  DeviceCredentials,
  DeviceInfo, DeviceInfoQuery,
  DeviceSearchQuery
} from '@app/shared/models/device.models';
import { EntitySubtype } from '@app/shared/models/entity-type.models';
import { AuthService } from '@core/auth/auth.service';
import { BulkImportRequest, BulkImportResult } from '@home/components/import-export/import-export.models';
import { PersistentRpc, RpcStatus } from '@shared/models/rpc.models';
import { File } from 'buffer';

@Injectable({
  providedIn: 'root'
})
export class DeviceService {

  constructor(
    private http: HttpClient
  ) { }

  public getDeviceInfosByQuery(deviceInfoQuery: DeviceInfoQuery, config?: RequestConfig): Observable<PageData<DeviceInfo>> {
    return this.http.get<PageData<DeviceInfo>>(`/api${deviceInfoQuery.toQuery()}`,
      defaultHttpOptionsFromConfig(config));
  }

  public getTenantDeviceInfos(pageLink: PageLink, type: string = '',
                              config?: RequestConfig): Observable<PageData<DeviceInfo>> {
    return this.http.get<PageData<DeviceInfo>>(`/api/tenant/deviceInfos${pageLink.toQuery()}&type=${type}`,
      defaultHttpOptionsFromConfig(config));
  }

  public getTenantDeviceInfosByDeviceProfileId(pageLink: PageLink, deviceProfileId: string = '',
                                               config?: RequestConfig): Observable<PageData<DeviceInfo>> {
    return this.http.get<PageData<DeviceInfo>>(`/api/tenant/deviceInfos${pageLink.toQuery()}&deviceProfileId=${deviceProfileId}`,
      defaultHttpOptionsFromConfig(config));
  }

  public getCustomerDeviceInfos(customerId: string, pageLink: PageLink, type: string = '',
                                config?: RequestConfig): Observable<PageData<DeviceInfo>> {
    return this.http.get<PageData<DeviceInfo>>(`/api/customer/${customerId}/deviceInfos${pageLink.toQuery()}&type=${type}`,
      defaultHttpOptionsFromConfig(config));
  }

  public getCustomerDeviceInfosByDeviceProfileId(customerId: string, pageLink: PageLink, deviceProfileId: string = '',
                                                 config?: RequestConfig): Observable<PageData<DeviceInfo>> {
    return this.http.get<PageData<DeviceInfo>>(`/api/customer/${customerId}/deviceInfos${pageLink.toQuery()}&deviceProfileId=${deviceProfileId}`,
      defaultHttpOptionsFromConfig(config));
  }

  public getDevice(deviceId: string, config?: RequestConfig): Observable<Device> {
    return this.http.get<Device>(`/api/device/${deviceId}`, defaultHttpOptionsFromConfig(config));
  }

  public getDevices(deviceIds: Array<string>, config?: RequestConfig): Observable<Array<Device>> {
    return this.http.get<Array<Device>>(`/api/devices?deviceIds=${deviceIds.join(',')}`, defaultHttpOptionsFromConfig(config));
  }

  public getDeviceInfo(deviceId: string, config?: RequestConfig): Observable<DeviceInfo> {
    return this.http.get<DeviceInfo>(`/api/device/info/${deviceId}`, defaultHttpOptionsFromConfig(config));
  }

  public saveDevice(device: Device, config?: RequestConfig): Observable<Device> {
    return this.http.post<Device>('/api/device', device, defaultHttpOptionsFromConfig(config));
  }

  public deleteDevice(deviceId: string, config?: RequestConfig) {
    return this.http.delete(`/api/device/${deviceId}`, defaultHttpOptionsFromConfig(config));
  }

  public getDeviceTypes(config?: RequestConfig): Observable<Array<EntitySubtype>> {
    return this.http.get<Array<EntitySubtype>>('/api/device/types', defaultHttpOptionsFromConfig(config));
  }

  public getDeviceCredentials(deviceId: string, sync: boolean = false, config?: RequestConfig): Observable<DeviceCredentials> {
    const url = `/api/device/${deviceId}/credentials`;
    if (sync) {
      const responseSubject = new ReplaySubject<DeviceCredentials>();
      const request = new XMLHttpRequest();
      request.open('GET', url, false);
      request.setRequestHeader('Accept', 'application/json, text/plain, */*');
      const jwtToken = AuthService.getJwtToken();
      if (jwtToken) {
        request.setRequestHeader('X-Authorization', 'Bearer ' + jwtToken);
      }
      request.send(null);
      if (request.status === 200) {
        const credentials = JSON.parse(request.responseText) as DeviceCredentials;
        responseSubject.next(credentials);
      } else {
        responseSubject.error(null);
      }
      return responseSubject.asObservable();
    } else {
      return this.http.get<DeviceCredentials>(url, defaultHttpOptionsFromConfig(config));
    }
  }

  public saveDeviceCredentials(deviceCredentials: DeviceCredentials, config?: RequestConfig): Observable<DeviceCredentials> {
    return this.http.post<DeviceCredentials>('/api/device/credentials', deviceCredentials, defaultHttpOptionsFromConfig(config));
  }

  public makeDevicePublic(deviceId: string, config?: RequestConfig): Observable<Device> {
    return this.http.post<Device>(`/api/customer/public/device/${deviceId}`, null, defaultHttpOptionsFromConfig(config));
  }

  public assignDeviceToCustomer(customerId: string, deviceId: string,
                                config?: RequestConfig): Observable<Device> {
    return this.http.post<Device>(`/api/customer/${customerId}/device/${deviceId}`, null, defaultHttpOptionsFromConfig(config));
  }

  public unassignDeviceFromCustomer(deviceId: string, config?: RequestConfig) {
    return this.http.delete(`/api/customer/device/${deviceId}`, defaultHttpOptionsFromConfig(config));
  }

  public sendOneWayRpcCommand(deviceId: string, requestBody: any, config?: RequestConfig): Observable<any> {
    return this.http.post<Device>(`/api/rpc/oneway/${deviceId}`, requestBody, defaultHttpOptionsFromConfig(config));
  }

  public sendTwoWayRpcCommand(deviceId: string, requestBody: any, config?: RequestConfig): Observable<any> {
    return this.http.post<Device>(`/api/rpc/twoway/${deviceId}`, requestBody, defaultHttpOptionsFromConfig(config));
  }

  public getPersistedRpc(rpcId: string, fullResponse = false, config?: RequestConfig): Observable<PersistentRpc> {
    return this.http.get<PersistentRpc>(`/api/rpc/persistent/${rpcId}`, defaultHttpOptionsFromConfig(config));
  }

  public deletePersistedRpc(rpcId: string, config?: RequestConfig) {
    return this.http.delete<PersistentRpc>(`/api/rpc/persistent/${rpcId}`, defaultHttpOptionsFromConfig(config));
  }

  public getPersistedRpcRequests(deviceId: string, pageLink: PageLink,
                                 rpcStatus?: RpcStatus, config?: RequestConfig): Observable<PageData<PersistentRpc>> {
    let url = `/api/rpc/persistent/device/${deviceId}${pageLink.toQuery()}`;
    if (rpcStatus && rpcStatus.length) {
      url += `&rpcStatus=${rpcStatus}`;
    }
    return this.http.get<PageData<PersistentRpc>>(url, defaultHttpOptionsFromConfig(config));
  }

  public findByQuery(query: DeviceSearchQuery,
                     config?: RequestConfig): Observable<Array<Device>> {
    return this.http.post<Array<Device>>('/api/devices', query, defaultHttpOptionsFromConfig(config));
  }

  public findByName(deviceName: string, config?: RequestConfig): Observable<Device> {
    return this.http.get<Device>(`/api/tenant/devices?deviceName=${deviceName}`, defaultHttpOptionsFromConfig(config));
  }

  public claimDevice(deviceName: string, claimRequest: ClaimRequest,
                     config?: RequestConfig): Observable<ClaimResult> {
    return this.http.post<ClaimResult>(`/api/customer/device/${deviceName}/claim`, claimRequest, defaultHttpOptionsFromConfig(config));
  }

  public unclaimDevice(deviceName: string, config?: RequestConfig) {
    return this.http.delete(`/api/customer/device/${deviceName}/claim`, defaultHttpOptionsFromConfig(config));
  }

  public assignDeviceToEdge(edgeId: string, deviceId: string,
                            config?: RequestConfig): Observable<Device> {
    return this.http.post<Device>(`/api/edge/${edgeId}/device/${deviceId}`,
      defaultHttpOptionsFromConfig(config));
  }

  public unassignDeviceFromEdge(edgeId: string, deviceId: string,
                                config?: RequestConfig) {
    return this.http.delete(`/api/edge/${edgeId}/device/${deviceId}`,
      defaultHttpOptionsFromConfig(config));
  }

  public getEdgeDevices(edgeId: string, pageLink: PageLink, type: string = '',
                        config?: RequestConfig): Observable<PageData<DeviceInfo>> {
    return this.http.get<PageData<DeviceInfo>>(`/api/edge/${edgeId}/devices${pageLink.toQuery()}&type=${type}`,
      defaultHttpOptionsFromConfig(config));
  }

  public bulkImportDevices(entitiesData: BulkImportRequest, config?: RequestConfig): Observable<BulkImportResult> {
    return this.http.post<BulkImportResult>('/api/device/bulk_import', entitiesData, defaultHttpOptionsFromConfig(config));
  }

  public downloadCsv(entityId: string, config?: any) {
    const url = `/api/device/${entityId}/csv/download`;
    this.http.get(url, {
      ...config,
      responseType: 'blob', // Set the responseType to 'blob'
      observe: 'response', // Include the full response in the result
    }).subscribe((response: any) => {
      if (response.status === 200 && response.body) {
        const blob = new Blob([response.body], { type: 'application/zip' });
        this.downloadFile(blob);
      } else {
        console.error('Failed to download File. Status:', response.status);
        // Handle the error case
      }
    }, (error: any) => {
      console.error('Failed to download File. Error:', error);
      // Handle the error case
    });
  }

  downloadFile(data: Blob) {
    const url = window.URL.createObjectURL(data);
    window.open(url); // Open the file in a new browser tab
  }
}






