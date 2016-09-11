FROM python

MAINTAINER John Harris <john@johnharris.io>

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

ENTRYPOINT ["python"]

CMD ["query_sportsradar.py"]


