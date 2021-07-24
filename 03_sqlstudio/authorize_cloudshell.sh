#!/bin/bash
gcloud sql instances patch flights2 \
	--authorized-networks $(wget -qO - http://ipecho.net/plain)/32
