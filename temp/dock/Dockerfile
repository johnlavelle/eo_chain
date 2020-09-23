FROM frolvlad/alpine-miniconda3
#RUN apt-get upgrade && apt-get update && apt-get install -y python3.7
RUN apk add --no-cache bash
ADD dock/hello.py /
CMD [ "python", "./hello.py" ]
