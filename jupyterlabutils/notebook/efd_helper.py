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
import asyncio
import pandas as pd

from jupyterlabutils.notebook import NotebookAuth

class EFD_client:
    """Class to handle connections and basic queries"""
    def __init__(self, efd_name, db_name='efd', port='443', path_to_creds=None):
        self.auth = NotebookAuth(path=path_to_creds)
        self.host, self.user, self.password = self.auth.getAuth(efd_name)
        self.client = aioinflux.InfluxDBClient(host=self.host, 
                                               port=port, 
                                               ssl=True, 
                                               username=username, 
                                               password=password,
                                               db=db_name)
        self.client.output = 'dataframe'

    def select_time_series(self, topic_name, fields, t1, t2, is_window=False):
        """Select a time series for a set of topics in a single subsystem"""

        ## TODO: make sure to take care of time zones.  Assume GMT by default?
        timespan = ''
        if not isinstance(t1, pd.TimeStamp):
            raise TypeError('The first time argument must be a time stamp')
        if isinstance(t2, pd.TimeStamp):
            timespan = f'time >= {t1.isoformat} and time <= {t2.isoformat}'
        elif isinstance(t2, pd.TimeDelta):
            if is_window:
                timespan = f'time >= {(t1 - t2/2).isoformat} and time <= {(t1 + t2/2).isoformat}'
            else:
                timespan = f'time >= {t1.isoformat} and time <= {(t2 + t2}.isoformat}'
        else:
            raise TypeError('The second time argument must be the time stamp for the end ' +
                            'or a time delta from the beginning')

        if isinstance(fields, str):
            fields = [fields,]
        elif isinstance(fields, bytes):
            fields = fields.decode()
            fields = [fields,]

        # Build query here
        if not base:
            raise ValueError(f'No subsystem specified')
        query = f'FROM {topic_name} select {", ".join(fields)}'
        if timespan:
            query = f'{query} WHERE {timespan}'

        # Do query
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.client.query(query))
