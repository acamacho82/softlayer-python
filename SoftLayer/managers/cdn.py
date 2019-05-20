"""
    SoftLayer.cdn
    ~~~~~~~~~~~~~
    CDN Manager/helpers

    :license: MIT, see LICENSE for more details.
"""
from datetime import datetime
from SoftLayer import exceptions
from SoftLayer import utils


class CDNManager(utils.IdentifierMixin, object):
    """Manage Content Delivery Networks in the account.

    See product information here:
    https://www.ibm.com/cloud/cdn
    https://cloud.ibm.com/docs/infrastructure/CDN?topic=CDN-about-content-delivery-networks-cdn-

    :param SoftLayer.API.BaseClient client: the client instance
    """

    def __init__(self, client):
        self.client = client
        self.cdn_configuration = self.client['Network_CdnMarketplace_Configuration_Mapping']
        self.cdn_path = self.client['SoftLayer_Network_CdnMarketplace_Configuration_Mapping_Path']
        self.cdn_metrics = self.client['Network_CdnMarketplace_Metrics']
        self.cdn_purge = self.client['SoftLayer_Network_CdnMarketplace_Configuration_Cache_Purge']

    def list_cdn(self, **kwargs):
        """Lists Content Delivery Networks for the active user.

        :param dict \\*\\*kwargs: header-level options (mask, limit, etc.)
        :returns: The list of CDN objects in the account
        """

        return self.cdn_configuration.listDomainMappings(**kwargs)

    def get_cdn(self, unique_id, **kwargs):
        """Retrieves the information about the CDN account object.

        :param int unique_id: The unique ID associated with the CDN.
        :param dict \\*\\*kwargs: header-level option (mask)
        :returns: The CDN object
        """

        cdn_list = self.cdn_configuration.listDomainMappingByUniqueId(unique_id, **kwargs)

        # The method listDomainMappingByUniqueId() returns an array but there is only 1 object
        return cdn_list[0]

    def get_origins(self, unique_id, **kwargs):
        """Retrieves list of origin pull mappings for a specified CDN account.

        :param int unique_id: The unique ID associated with the CDN.
        :param dict \\*\\*kwargs: header-level options (mask, limit, etc.)
        :returns: The list of origin paths in the CDN object.
        """

        return self.cdn_path.listOriginPath(unique_id, **kwargs)

    def add_origin(self, unique_id, path, origin_address, origin_type="HOST_SERVER", header=None,
                   port=80, protocol='HTTP', bucket_name=None, file_extensions=None,
                   optimize_for="General web delivery", cache_query="include all"):
        """

        :param unique_id:
        :param path:
        :param origin_address:
        :param origin_type:
        :param header:
        :param port:
        :param protocol:
        :param bucket_name:
        :param file_extensions:
        :param optimize_for:
        :param cache_query:
        :return:
        """

        new_origin = {
            'uniqueId': unique_id,
            'path': path,
            'origin': origin_address,
            'originType': origin_type,
            'httpPort': port,
            'protocol': protocol,
            'performanceConfiguration': optimize_for,
            'cacheKeyQueryRule': cache_query
        }

        if header:
            new_origin['header'] = header

        if origin_type == 'OBJECT_STORAGE':
            if bucket_name:
                new_origin['bucketName'] = bucket_name
            else:
                raise exceptions.SoftLayerError("Bucket name is required when the origin type is OBJECT_STORAGE")

            if file_extensions:
                new_origin['fileExtension'] = file_extensions

        origin = self.cdn_path.createOriginPath(new_origin)

        # The method createOriginPath() returns an array but there is only 1 object
        return origin[0]

    def remove_origin(self, unique_id, path):
        """Removes an origin pull mapping with the given origin pull ID.

        :param int unique_id: The unique ID associated with the CDN.
        :param str path: The origin path to delete.
        :returns: A string value
        """

        return self.cdn_path.deleteOriginPath(unique_id, path)

    def purge_content(self, unique_id, path):
        """Purges a URL or path from the CDN.

        :param int unique_id: The unique ID associated with the CDN.
        :param str path: A string of url or path that should be purged.
        :returns: A Container_Network_CdnMarketplace_Configuration_Cache_Purge array object
        """

        return self.cdn_purge.createPurge(unique_id, path)

    def get_usage_metrics(self, unique_id, start_date=None, end_date=None, days=30, frequency="aggregate"):
        """Retrieves the cdn usage metrics.

        It uses the 'days' argument if start_date and end_date are None.

        :param int unique_id: The CDN uniqueId from which the usage metrics will be obtained.
        :param str start_date: "%Y-%m-%d %H:%M:%S" formatted string.
        :param str end_date: "%Y-%m-%d %H:%M:%S" formatted string.
        :param int days: Last N days, default days is 30.
        :param str frequency: It can be day, week, month and aggregate. The default is "aggregate".
        :returns: A Container_Network_CdnMarketplace_Metrics object
        """

        _start = utils.days_to_datetime(days)
        _end = utils.days_to_datetime(0)

        if start_date and end_date:
            try:
                _start = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                _end = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise
        elif bool(start_date) != bool(end_date):
            raise ValueError("start_date or end_date is None, both need to be strings or None.")

        _start_date = utils.timestamp(_start)
        _end_date = utils.timestamp(_end)

        usage = self.cdn_metrics.getMappingUsageMetrics(unique_id, _start_date, _end_date, frequency)

        # The method getMappingUsageMetrics() returns an array but there is only 1 object
        return usage[0]
