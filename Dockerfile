FROM python:3.7-alpine as base

LABEL maintainer="Arkadiusz Dzięgiel <arkadiusz.dziegiel@glorpen.pl>"

FROM base as build

COPY README.rst setup.py /srv/
COPY src /srv/src

RUN pip install --compile --root /srv/image /srv \
    && rm -rf /root/.cache

FROM base

COPY --from=build /srv/image/ /

ENTRYPOINT ["glorpen-deduplication"]
