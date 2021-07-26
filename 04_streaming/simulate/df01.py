#!/usr/bin/env python3

# Copyright 2019 Google Inc.
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

if __name__ == '__main__':
   with beam.Pipeline('DirectRunner') as pipeline: # 'DirectRunner' -> local에서 실행하겠다.

      airports = (pipeline
         | beam.io.ReadFromText('airports.csv.gz') # 이름 파일 읽어
         | beam.Map(lambda line: next(csv.reader([line]))) # 한 줄씩 읽겠다.
         | beam.Map(lambda fields: (fields[0], (fields[21], fields[26]))) # 원하는 필드 지정 ( 공항 공유번호, (위도, 경도))
      )

      (airports 
         | beam.Map(lambda airport_data: '{},{}'.format(airport_data[0], ','.join(airport_data[1])) ) # 튜플 해제하고 x,x,x 형태로 변환
         | beam.io.WriteToText('extracted_airports')  # 이름 파일 출력
      )

      pipeline.run()
