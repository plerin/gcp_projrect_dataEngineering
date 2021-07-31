#!/usr/bin/env python

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

from __future__ import print_function
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

def as_utc(date, hhmm, tzone): # PARAM : 2015-01-01, 0512, America/Anchorage  형태로 들어와
   try:
      if len(hhmm) > 0 and tzone is not None:
         import datetime, pytz
         loc_tz = pytz.timezone(tzone)
         loc_dt = loc_tz.localize(datetime.datetime.strptime(date,'%Y-%m-%d'), is_dst=False)
         # can't just parse hhmm because the data contains 2400 and the like ...
         loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
         utc_dt = loc_dt.astimezone(pytz.utc)
         return utc_dt.strftime('%Y-%m-%d %H:%M:%S'), loc_dt.utcoffset().total_seconds() # tocoffset() : 표준 시에서 로컬(각 나라)시에 맞는 offset값을 리턴
      else:
         return '',0 # empty string corresponds to canceled flights
   except ValueError as e:
      print('{} {} {}'.format(date, hhmm, tzone))
      raise e

def add_24h_if_before(arrtime, deptime):  # arrtime을 기준으로 1일 뒤(24시간) date 리턴
   import datetime
   if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
      adt = datetime.datetime.strptime(arrtime, '%Y-%m-%d %H:%M:%S') # arrtime을 원하는 포맷으로 변경('%Y-%m-%d %H:%M:%S')
      adt += datetime.timedelta(hours=24) # 변경된 date(adt)에 1일 더하기(hours=24)
      return adt.strftime('%Y-%m-%d %H:%M:%S')  # adt에 1일 더한 값을 원하는 형태로 리턴('%Y-%m-%d %H:%M:%S')
   else:
      return arrtime

def tz_correct(line, airport_timezones): # airport_timezones는 df03에서 return 받은 데이터 '1000101,58.10944444,-152.90666667,America/Anchorage'
   fields = line.split(',')
   if fields[0] != 'FL_DATE' and len(fields) == 27:
      # convert all times to UTC
      dep_airport_id = fields[6]    # 출발 공항 ID
      arr_airport_id = fields[10]   # 도착 공항 ID
      dep_timezone = airport_timezones[dep_airport_id][2]   # 출발 공항 ID의 TIMEZONE => EX)  America/Anchorage 가 담긴다.
      arr_timezone = airport_timezones[arr_airport_id][2]   # 도착 공항 ID - TIMEZONE
      
      for f in [13, 14, 17]: #crsdeptime, deptime, wheelsoff
         fields[f], deptz = as_utc(fields[0], fields[f], dep_timezone) # [f]의 값을 utc 표준시로 변경하여 저장, 
      for f in [18, 20, 21]: #wheelson, crsarrtime, arrtime
         fields[f], arrtz = as_utc(fields[0], fields[f], arr_timezone)
      
      for f in [17, 18, 20, 21]: # wheelsoff, wheelson, crsarrtime, arrtime _ when arrtime smaller than deptime because line 64-67 then calibrate data
         fields[f] = add_24h_if_before(fields[f], fields[14])  # 도착관련된 시간정보가 출발시간보다 빠른경우 1일 추가(전처리)

      fields.extend(airport_timezones[dep_airport_id])   # 출발 공항 정보(좌표/timezone)을 추가
      fields[-1] = str(deptz) # 출발 공항 정보의 timezone을 offset시간으로 변경
      fields.extend(airport_timezones[arr_airport_id])   # 도착 공항 정보(좌표/timezone)을 추가
      fields[-1] = str(arrtz) # 도착 공항 정보의 timezone을 offset시간으로 변경 ex) +09:00(8900000, 정확한 값은 아니고 예시임) / -09:00(-89000000)

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
