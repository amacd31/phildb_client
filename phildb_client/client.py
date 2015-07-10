import types
import pandas as pd

class PhilDBClient(object):

    def __init__(self, server):
        self.server = server

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
        url = self.server + '/{0}/{1}.json'.format(identifier, freq)

        num_attrs = len(kwargs)
        if num_attrs > 0:
            url += '?'

        for key, value in kwargs.items():
            url += key + '=' + value
            num_attrs -= 1
            if num_attrs > 0:
                url += '&'

        return pd.read_json(url, typ='ser')

    def read_all(self, freq, excludes = None, **kwargs):
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
        raise NotImplemented()

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

    def ts_list(self, **kwargs):
        """
            Returns list of primary ID for all timeseries records.

            :param kwargs: Restrict to records associated with this the kwargs
                attributes supplied. (Optional).
            :type kwargs: kwargs
            :returns: list(string) -- Sorted list of timeseries identifiers.
        """
        url = self.server + '/ts_list.json'

        num_attrs = len(kwargs)
        if num_attrs > 0:
            url += '?'

        for key, value in kwargs.items():
            url += key + '=' + value
            num_attrs -= 1
            if num_attrs > 0:
                url += '&'

        return pd.read_json(url, typ='ser').values

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
        raise NotImplemented()

    def list_sources(self):
        """
            Returns list of source IDs for all sources.

            :returns: list(string) -- Sorted list of source identifiers.
        """
        raise NotImplemented()

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
