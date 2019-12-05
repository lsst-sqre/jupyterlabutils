# This file is part of jupyterlabutils.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import aioinflux
from collections import Counter
import logging
import pandas as pd

from jupyterlabutils.notebook import NotebookAuth


class EfdClient:
    """Class to handle connections and basic queries"""
    def __init__(self, efd_name, db_name='efd', port='443', path_to_creds='~/.lsst/notebook_auth.yaml'):
        self.db_name = db_name
        self.auth = NotebookAuth(path=path_to_creds)
        self.host, self.user, self.password = self.auth.get_auth(efd_name)
        self.client = aioinflux.InfluxDBClient(host=self.host,
                                               port=port,
                                               ssl=True,
                                               username=self.user,
                                               password=self.password,
                                               db=db_name,
                                               mode='async')  # mode='blocking')
        self.client.output = 'dataframe'

    async def get_topics(self):
        topics = await self.client.query('SHOW MEASUREMENTS')
        return topics['name'].tolist()

    async def get_fields(self, topic_name):
        fields = await self.client.query(f'SHOW FIELD KEYS FROM "{self.db_name}"."autogen"."{topic_name}"')
        return fields['fieldKey'].tolist()

    async def select_time_series(self, topic_name, fields, t1, t2, is_window=False):
        """Select a time series for a set of topics in a single subsystem"""
        if not t1.tz:
            raise ValueError('No timezone information found.  Timezone must be set.')
        if not t1.tz.zone == 'UTC':
            logging.warn('Timestamps must be in UTC.  Converting...')
            t1 = t1.tz_convert(tz='UTC')

        if not isinstance(t1, pd.Timestamp):
            raise TypeError('The first time argument must be a time stamp')
        if isinstance(t2, pd.Timestamp):
            t2 = t2.tz_convert(tz='UTC')
            start = t1.isoformat()
            end = t2.isoformat()
        elif isinstance(t2, pd.Timedelta):
            if is_window:
                start = (t1 - t2/2).isoformat()
                end = (t1 + t2/2).isoformat()
            else:
                start = t1.isoformat()
                end = (t1 + t2).isoformat()
        else:
            raise TypeError('The second time argument must be the time stamp for the end ' +
                            'or a time delta.')

        timespan = f"time >= '{start}' AND time <= '{end}'"

        if isinstance(fields, str):
            fields = [fields, ]
        elif isinstance(fields, bytes):
            fields = fields.decode()
            fields = [fields, ]

        # Build query here
        query = f'SELECT {", ".join(fields)} FROM "{self.db_name}"."autogen"."{topic_name}" WHERE {timespan}'

        # Do query
        ret = await self.client.query(query)
        if not isinstance(ret, pd.DataFrame) and not ret:
            # aioinflux returns an empty dict for an empty query
            ret = pd.DataFrame()
        return ret

    def _make_fields(self, fields, base_fields):
        # Count the number of occurences of each base field
        count = Counter([bfield for bfield in base_fields for field in fields if bfield in field])
        # Make sure all the base fields occur the same number of times
        if len(set(count.values())) != 1:
            raise ValueError('All array fields are not the same length')
        n = set(count.values()).pop()  # Get number of elements
        ret = {}
        for bfield in base_fields:
            for i in range(n):
                fname = f'{bfield}{i}'
                if fname not in fields:
                    raise ValueError(f'Field {fname} not in list of possible fields')
                ret.setdefault(bfield, default=[]).append(fname)
        return ret, n

    async def select_packed_time_series(self, topic_name, base_fields, t1, t2,
                                        is_window=False, ref_timestamp_col="cRIO_timestamp"):
        """Select fields that are time samples and unpack them into a dataframe"""
        fields = await self.get_fields(topic_name)
        qfields, els = self._make_fields(fields, base_fields)
        field_list = []
        for k in qfields:
            field_list += qfields[k]
        result = await self.select_time_series(topic_name, field_list+[ref_timestamp_col, ],
                                               t1, t2, is_window=is_window)
        times = []
        timestamps = []
        vals = {}
        step = 1./els
        for row in result.itertuples():
            for i in range(els):
                t = getattr(row, ref_timestamp_col)
                times.append(t + i*step)
                timestamps.append((pd.Timestamp(t, unit='s', tz='UTC') + pd.Timedelta(i*step, unit='s')))
                for k in qfields:
                    fld = f'{k}{i}'
                    if fld not in qfields[k]:
                        raise ValueError(f'{fld} not in field list')
                    vals.setdefault(k, default=[]).append(getattr(row, fld))
        return pd.DataFrame(vals.update({'times': times}), index=timestamps)


def resample(df1, df2, sort_type='time'):
    df = df1.append(df2, sort=False)  # Sort in this context does not sort the data
    df = df.sort_index()
    return df.interpolate(type=sort_type)
