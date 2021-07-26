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
      tz = tf.timezone_at(lng=float(lon), lat=float(lat)) # lon/lat을 사용하여 timezone을 구함 
      if tz is None:
         tz = 'UTC'
      return (lat, lon, tz) # 반환 데이터 형태
   except ValueError:
      return (lat, lon, 'TIMEZONE') # header

if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline:

      airports = (pipeline
         | beam.io.ReadFromText('airports.csv.gz')
         | beam.Map(lambda line: next(csv.reader([line])))  # 한 줄씩 읽겠다.
         | beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
      )

      airports | beam.Map(lambda f: '{},{}'.format(f[0], ','.join(f[1])) )| beam.io.textio.WriteToText('airports_with_tz') # 튜플 형식 -> x,x,x,x 형태 변환 및 데이터 저장

      pipeline.run()
