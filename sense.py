import sense_energy
import datetime
import pandas
# pip3 install azure-kusto-data azure-kusto-ingest sense-energy pandas

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    KustoIngestClient,
    IngestionProperties,
    StreamDescriptor,
    DataFormat,
    ReportLevel,
    IngestionMappingType,
    KustoStreamingIngestClient,
)

from azure.kusto.ingest.status import KustoIngestStatusQueues

cluster = "{yourclusternamehere}"
client_id = "{yourclientidhere}"
client_secret = "{yoursecrethere}"
authority_id = "{yourAADTenanthere}"

kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
client = KustoStreamingIngestClient(kcsb=kcsb)

ingestion_props = IngestionProperties(
    database="{yourADXDatabasehere}",
    table="{yourADXTablehere}",
    data_format=DataFormat.CSV
)

while True:
    try:
        sense = sense_energy.Senseable()
        sense.authenticate("{youruseridhere}", "{yourpasswordhere}")
        sensegenerator = sense.get_realtime_stream()

        #    .create-merge table power (volt1: real, volt2: real, watts1: real, watts2: real, hz: real, totalc: int, ["time"]: long, currentDateTime: datetime, id: string, name: string, icon: string, watts: real, c: int)
        fields = ["volt1", "volt2", "watts1", "watts2", "hz", "totalc", "time", "currentDateTime", "id", "name", "icon", "watts", "c"]

        for sensedata in sensegenerator:
            # {'voltage': [120.37093353271484, 120.45533752441406], 'frame': 7858380, 'devices': [{'id': 'M532Vg2f', 'name': 'Downstairs AC', 'icon': 'ac', 'tags': {'Alertable': 'true', 'AlwaysOn': 'false', 'DateCreated': '2018-05-19T23:53:49.981Z', 'DateFirstUsage': '2018-03-24', 'DefaultLocation': None, 'DefaultMake': None, 'DefaultModel': None, 'DefaultUserDeviceType': 'AC', 'DeployToMonitor': 'true', 'DeviceListAllowed': 'true', 'Mature': 'true', 'MergedDevices': '03170c48,4a2f489c,3fcc985a', 'ModelCreatedVersion': '50', 'name_useredit': 'true', 'OriginalName': 'AC', 'PeerNames': [], 'Pending': 'false', 'Revoked': 'false', 'TimelineAllowed': 'true', 'TimelineDefault': 'false', 'Type': 'CentralAC', 'user_editable': 'true', 'UserDeletable': 'true', 'UserDeviceType': 'AC', 'UserDeviceTypeDisplayString': 'AC', 'UserEditable': 'true', 'UserEditableMeta': 'true', 'UserMergeable': 'true', 'Virtual': 'true'}, 'attrs': [], 'w': 3782.74072265625, 'c': 37}, {'id': 'unknown', 'name': 'Other', 'icon': 'home', 'tags': {'DefaultUserDeviceType': 'Unknown', 'DeviceListAllowed': 'true', 'TimelineAllowed': 'false', 'UserDeviceType': 'Unknown', 'UserDeviceTypeDisplayString': 'Unknown', 'UserEditable': 'false'}, 'attrs': [], 'w': 549.2903442382812, 'c': 5}, {'id': 'always_on', 'name': 'Always On', 'icon': 'alwayson', 'tags': {'DefaultUserDeviceType': 'AlwaysOn', 'DeviceListAllowed': 'true', 'TimelineAllowed': 'false', 'UserDeviceType': 'AlwaysOn', 'UserDeviceTypeDisplayString': 'AlwaysOn', 'UserEditable': 'false'}, 'attrs': [], 'w': 531.0, 'c': 5}, {'id': 'f0311af3', 'name': 'Kitchen Fridge', 'icon': 'fridge', 'tags': {'Alertable': 'true', 'AlwaysOn': 'false', 'DateCreated': '2018-07-28T18:40:28.064Z', 'DateFirstUsage': '2018-07-23', 'DefaultLocation': None, 'DefaultMake': None, 'DefaultModel': None, 'DefaultUserDeviceType': 'Fridge', 'DeployToMonitor': 'true', 'DeviceListAllowed': 'true', 'Mature': 'true', 'ModelCreatedVersion': '66', 'ModelUpdatedVersion': '182', 'name_useredit': 'true', 'OriginalName': 'Fridge 6', 'PeerNames': [{'Name': 'Fridge', 'UserDeviceType': 'Fridge', 'Percent': 99.0, 'Icon': 'fridge', 'UserDeviceTypeDisplayString': 'Fridge'}], 'Pending': 'false', 'Revoked': 'false', 'TimelineAllowed': 'true', 'TimelineDefault': 'true', 'Type': 'Refrigerator', 'user_editable': 'true', 'UserDeletable': 'true', 'UserDeviceType': 'Fridge', 'UserDeviceTypeDisplayString': 'Fridge', 'UserEditable': 'true', 'UserEditableMeta': 'true', 'UserMergeable': 'true'}, 'attrs': [], 'w': 180.69590759277344, 'c': 1}, {'id': '8f07ee2c', 'name': 'Fridge 7', 'icon': 'fridge', 'tags': {'Alertable': 'true', 'AlwaysOn': 'false', 'DateCreated': '2019-03-02T00:09:50.648Z', 'DateFirstUsage': '2018-12-29', 'DefaultLocation': None, 'DefaultMake': None, 'DefaultModel': None, 'DefaultUserDeviceType': 'Fridge', 'DeployToMonitor': 'true', 'DeviceListAllowed': 'true', 'Mature': 'true', 'ModelCreatedVersion': '108', 'ModelUpdatedVersion': '182', 'name_useredit': 'false', 'OriginalName': 'Fridge 7', 'PeerNames': [{'Name': 'Freezer', 'UserDeviceType': 'Freezer', 'Percent': 100.0, 'Icon': 'freezer', 'UserDeviceTypeDisplayString': 'Freezer'}], 'Pending': 'false', 'Revoked': 'false', 'TimelineAllowed': 'true', 'TimelineDefault': 'false', 'Type': 'Refrigerator', 'user_editable': 'true', 'UserDeletable': 'true', 'UserDeviceType': 'Fridge', 'UserDeviceTypeDisplayString': 'Fridge', 'UserEditable': 'true', 'UserEditableMeta': 'true', 'UserMergeable': 'true'}, 'attrs': [], 'w': 134.72955322265625, 'c': 1}], 'deltas': [], 'channels': [3036.6005859375, 2141.85595703125], 'hz': 59.9677848815918, 'w': 5178.45654296875, 'c': 51, '_stats': {'brcv': 1596138278.355959, 'mrcv': 1596138278.389, 'msnd': 1596138278.389}, 'd_w': 5178, 'epoch': 1596138277}
            volt1 =     float(sensedata['voltage'][0])
            volt2 =     float(sensedata['voltage'][1])
            watts1 =    float(sensedata['channels'][0])
            watts2 =    float(sensedata['channels'][1])
            hz =        int(sensedata['hz'])
            totalc =    int(sensedata['c'])
            currentT =  int(sensedata['epoch'])
            currentDT = datetime.datetime.now()
            rows = []

            deviceInfo = sensedata['devices']
            for device in deviceInfo:
                id =    str(device['id'])
                name =  str(device['name'])
                icon =  str(device['icon'])
                watts = int(device['w'])
                c =     int(device['c'])
                rows.append([volt1, volt2, watts1, watts2, hz, totalc, currentT, currentDT, id, name, icon, watts, c])
            
            df = pandas.DataFrame(data=rows, columns=fields)
            client.ingest_from_dataframe(df, ingestion_properties=ingestion_props)
    except Exception as ex:
        print(str(datetime.datetime.now()) + " " + str(ex))
