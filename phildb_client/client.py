import types
import pandas as pd
import logging
try:
    from urllib import urlencode
    from urllib2 import urlopen
except:
    from urllib.parse import urlencode
    from urllib.request import urlopen

logger = logging.getLogger("phildb_client")

class PhilDBClient(object):

    def __init__(self, server, format = 'msgpack'):
        """
            Creates a PhilDB client object that reads from the specified PhilDB server.

            Defaults to reading the msgpack format. The data format can be set to
            'json' instead if 'msgpack' is unable to be used (e.g. if using pandas < 0.17).
            The msgpack serialisation is using the experimental pandas implementation, so
            while it performs well and makes sense to use (hence being the default) the
            option to use JSON is available if compatiblity errors arise.

            :param server: URL of the PhilDB server to read from.
            :type server: string
            :param format: Data format to use when reading from the PhilDB server. Default 'msgpack'.
            :type format: string
        """
        self.server = server

        if format in ['msgpack', 'json']:
            self.format = format
        else:
            raise NotImplementedError('Unsupported format: {0}'.format(self.format))

    def help(self):
        """
            List methods of the TSDB class with the first line of their docstring.
        """
        for method in sorted(dir(self)):
            if method.startswith("_"):
                continue
            if not isinstance(getattr(self, method), types.MethodType):
                continue

            docstring = getattr(self, method).__doc__
            if docstring is None:
                short_string = ''
            else:
                docstring = docstring.split('\n')
                short_string = docstring[0].strip()
                if short_string == '':
                    short_string = docstring[1].strip()
            print("{0}: {1}".format(method, short_string))

    def version(self):
        """
            Returns the version number of the database schema.

            :returns: string -- Schema version.
        """
        raise NotImplemented()

    def __attach_kwargs_to_url(self, url, kwargs):

        num_attrs = len(kwargs)
        if num_attrs > 0:
            url += '?'

        url += urlencode(kwargs, True)
        logger.debug(url)

        return url

    def read(self, identifier, freq, **kwargs):
        """
            Read the entire timeseries record for the requested timeseries instance.

            :param identifier: Identifier of the timeseries.
            :type identifier: string
            :param freq: Timeseries data frequency.
            :type freq: string
            :param kwargs: Attributes to match against timeseries instances (e.g. source, measurand).
            :type kwargs: kwargs

            :returns: pandas.DataFrame -- Timeseries data.
        """
        url = self.__attach_kwargs_to_url(
                self.server + '/{0}/{1}.{2}'.format(identifier, freq, self.format),
                kwargs
            )

        if self.format == 'msgpack':
            return pd.read_msgpack(urlopen(url))
        elif self.format == 'json':
            return pd.read_json(urlopen(url))
        else:
            raise NotImplementedError('Unsupported format: {0}'.format(self.format))

    def read_all(self, freq, **kwargs):
        """
            Read the entire timeseries record for all matching timeseries instances.
            Optionally exclude timeseries from the final DataFrame by specifying IDs in the exclude argument.

            :param identifier: Identifier of the timeseries.
            :type identifier: string
            :param freq: Timeseries data frequency.
            :type freq: string
            :param excludes: IDs of timeseries to exclude from final DataFrame.
            :type excludes: array[string]
            :param kwargs: Attributes to match against timeseries instances (e.g. source, measurand).
            :type kwargs: kwargs

            :returns: pandas.DataFrame -- Timeseries data.
        """
        url = self.__attach_kwargs_to_url(
                self.server + '/read_all/{0}.{1}'.format(freq, self.format),
                kwargs
            )

        if self.format == 'msgpack':
            return pd.read_msgpack(urlopen(url))
        elif self.format == 'json':
            return pd.read_json(urlopen(url))
        else:
            raise NotImplementedError('Unsupported format: {0}'.format(self.format))

    def read_dataframe(self, identifiers, freq, **kwargs):
        """
            Read the entire timeseries record for the requested timeseries instances.

            :param identifiers: Identifiers of the timeseries to read into a DataFrame.
            :type identifiers: array[string]
            :param freq: Timeseries data frequency.
            :type freq: string
            :param kwargs: Attributes to match against timeseries instances (e.g. source, measurand).
            :type kwargs: kwargs

            :returns: pandas.DataFrame -- Timeseries data.
        """
        raise NotImplemented()

    def __get_list(self, list_name, kwargs):

        url = self.__attach_kwargs_to_url(
                self.server + '/list/{0}.{1}'.format(list_name, self.format),
                kwargs
            )

        if self.format == 'msgpack':
            return pd.read_msgpack(urlopen(url)).values.tolist()
        elif self.format == 'json':
            return pd.read_json(urlopen(url)).values.tolist()
        else:
            raise NotImplementedError('Unsupported format: {0}'.format(self.format))

    def ts_list(self, **kwargs):
        """
            Returns list of primary ID for all timeseries records.

            :param kwargs: Restrict to records associated with this the kwargs
                attributes supplied. (Optional).
            :type kwargs: kwargs
            :returns: list(string) -- Sorted list of timeseries identifiers.
        """
        return self.__get_list('timeseries', kwargs)

    def list_ids(self):
        """
            Returns list of timeseries IDs for all timeseries records.

            :returns: list(string) -- Sorted list of timeseries identifiers.
        """
        raise NotImplemented()

    def list_timeseries_instances(self, **kwargs):
        """
            Returns list of timeseries instances for all instance records.

            Can filter by using keyword arguments.

            :returns: list(string) -- Sorted list of timeseries instances.
        """
        raise NotImplemented()

    def list_measurands(self):
        """
            Returns list of measurand short IDs for all measurand records.

            :returns: list(string) -- Sorted list of timeseries identifiers.
        """
        return self.__get_list('measurands', {})

    def list_sources(self):
        """
            Returns list of source IDs for all sources.

            :returns: list(string) -- Sorted list of source identifiers.
        """
        return self.__get_list('sources', {})

    def read_metadata(self, ts_id, freq, **kwargs):
        """
            Returns the metadata that was associated with an initial TimeseriesInstance.

            :param identifier: Identifier of the timeseries.
            :type identifier: string
            :returns: string -- The initial metadata that was recorded on
                instance creation.
        """
        raise NotImplemented()

    def __str__(self):
        return self.tsdb_server
