FROM ubuntu:19.10

RUN apt-get update && apt-get install -y \
    python3-pip git binutils libproj-dev gdal-bin && \
    pip3 install --upgrade pip && \
    pip3 install pyproj numpy docopt zarr s3fs&& \
    pip3 install -e git+https://github.com/techworksmarine/satpy.git#egg=satpy
COPY . /app
WORKDIR /app
CMD ["python3", "eo_chain/process.py"]

#CMD ["cat", "/etc/os-release"]
#CMD ["/bin/bash"]

