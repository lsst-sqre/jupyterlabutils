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
import pandas as pd

from .find_credentials import cred_by_secret, cred_by_env, cred_by_file

class EFD_client:
    """Class to handle connections and basic queries"""
    def __init__(self, endpoint=None, username=None, password=None):
        pass

    def select_time_series(self, topic_names, t1, t2, is_window=False):
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

        # Do the actual query
        base = None
        topics = []
        for n in topic_names:
            parts = '.'.split(n)  # Split the namespace from the topic name
            if len(parts) < 2:
                raise ValueError(f'Topic names must be fully qualified: {n}')
            if not base:
                base = '.'.join(parts[:-1])
                topics.append(parts[-1])
            else:
                tmp_base = '.'.join(parts[:-1])
                if base != tmp_base:
                    raise ValueError(f'Topics must be from the same subsystem: ' +
                                     '{base} and {tmp_base} do not match')
                topics.append(parts[-1])

        # Build query here
