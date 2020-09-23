#!/bin/bash
#rsync -r  /data/ '/media/jlavelle/Seagate Expansion Drive/TWM/data'
rsync -rv /home/jlavelle/Documents/ '/media/jlavelle/Seagate Expansion Drive/TWM/Documents/'
#rsync -rv --exclude='.git/' /home/jlavelle/code/ '/media/jlavelle/Seagate Expansion Drive/TWM/code/'
#rsync -rv /home/jlavelle/.ssh/ '/media/jlavelle/Seagate Expansion Drive/TWM/ssh/'

rsync -Pav -e  "ssh -i ~/.ssh/central-europe.pem" ubuntu@ec2-54-93-234-246.eu-central-1.compute.amazonaws.com:/home/ubuntu/notebooks .
rsync -Pav -e  "ssh -i ~/.ssh/central-europe.pem" ubuntu@3.127.229.160:/data/sentinel2/in .