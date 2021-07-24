#!/bin/bash
gcloud sql instances patch flights2 \
	--authorized-networks $(wget -qO - http://ipecho.net/plain)/32 # ipecho.net에 요청을 보내 요청자의 IP주소를 반환받는다.(공인IP주소를 알아내는 방법이다.)
