#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam
import csv

def addtimezone(lat, lon):
   try:
      import timezonefinder
      tf = timezonefinder.TimezoneFinder()
      return (lat, lon, tf.timezone_at(lng=float(lon), lat=float(lat)))
      #return (lat, lon, 'America/Los_Angeles') # FIXME
   except ValueError:
      return (lat, lon, 'TIMEZONE') # header

def as_utc(date, hhmm, tzone):
   try:
      if len(hhmm) > 0 and tzone is not None:   # 데이터가 올바른지 확인
         import datetime, pytz
         loc_tz = pytz.timezone(tzone) # 입력한 tzone의 timezone을 갖고와 
         loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False) # 입력 데이터 timezone local에 맞도록 dataformat('%Y-%m-%d') 맞춰
         # can't just parse hhmm because the data contains 2400 and the like ...
         loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:])) # 시간은 입력된 hhmm 형식 데이터를 통해 hours/minutes로 나눠 입력
         utc_dt = loc_dt.astimezone(pytz.utc)   # loc_dt 데이터를 'utc' timezone올 변경
         return utc_dt.strftime('%Y-%m-%d %H:%M:%S') # 지정 포맷('%Y-%m-%d %H:%M:%S')으로 반환
      else:
         return '' # empty string corresponds to canceled flights
   except ValueError as e:
      print('{} {} {}'.format(date, hhmm, tzone))
      raise e

def tz_correct(line, airport_timezones):
   fields = line.split(',')
   if fields[0] != 'FL_DATE' and len(fields) == 27:
      # convert all times to UTC
      dep_airport_id = fields[6]
      arr_airport_id = fields[10]
      dep_timezone = airport_timezones[dep_airport_id][2]
      arr_timezone = airport_timezones[arr_airport_id][2]

      for f in [13, 14, 17]: #crsdeptime, deptime, wheelsoff
         fields[f] = as_utc(fields[0], fields[f], dep_timezone)
      for f in [18, 20, 21]: #wheelson, crsarrtime, arrtime
         fields[f] = as_utc(fields[0], fields[f], arr_timezone)

      yield ','.join(fields)

if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline:

      airports = (pipeline
         | 'airports:read' >> beam.io.ReadFromText('airports.csv.gz')
         | 'airports:fields' >> beam.Map(lambda line: next(csv.reader([line])))
         | 'airports:tz' >> beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
      )

      flights = (pipeline
         | 'flights:read' >> beam.io.ReadFromText('201501_part.csv')
         | 'flights:tzcorr' >> beam.FlatMap(tz_correct, beam.pvalue.AsDict(airports))
      )

      flights | beam.io.textio.WriteToText('all_flights')

      pipeline.run()
